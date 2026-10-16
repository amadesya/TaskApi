using System.Text;
using System.Text.Json;
using System.Threading.Channels;
using RabbitMQ.Client;

namespace TaskApi;

// Интерфейс нужен для подмены в тестах
public interface IEventPublisher
{
    ValueTask PublishAsync(TaskCompletedEvent evt);
}

public class RabbitMqPublisher : IEventPublisher
{
    private readonly Channel<TaskCompletedEvent> _channel;

    public RabbitMqPublisher(Channel<TaskCompletedEvent> channel)
    {
        _channel = channel;
    }

    public ValueTask PublishAsync(TaskCompletedEvent evt)
    {
        // Пишем в in-memory канал, не блокируем запрос
        return _channel.Writer.TryWrite(evt)
            ? ValueTask.CompletedTask
            : new ValueTask(Task.FromException(new InvalidOperationException("Очередь событий переполнена")));
    }
}

public class RabbitMqBackground : BackgroundService
{
    private readonly Channel<TaskCompletedEvent> _channel;
    private readonly IConfiguration _config;
    private readonly ILogger<RabbitMqBackground> _logger;

    private IConnection? _connection;
    private IChannel? _rabbitChannel;

    private const string Exchange = "task.events";
    private const string RoutingKey = "task.completed";

    public RabbitMqBackground(
        Channel<TaskCompletedEvent> channel,
        IConfiguration config,
        ILogger<RabbitMqBackground> logger)
    {
        _channel = channel;
        _config = config;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await ConnectAsync(stoppingToken);

        await foreach (var evt in _channel.Reader.ReadAllAsync(stoppingToken))
        {
            try
            {
                if (_rabbitChannel == null)
                    await ConnectAsync(stoppingToken);

                var body = JsonSerializer.SerializeToUtf8Bytes(evt, new JsonSerializerOptions
                {
                    PropertyNamingPolicy = JsonNamingPolicy.CamelCase
                });

                // HACK: не успел, доделал бы retry с exponential backoff при ошибке публикации
                await _rabbitChannel!.BasicPublishAsync(
                    exchange: Exchange,
                    routingKey: RoutingKey,
                    body: body,
                    cancellationToken: stoppingToken);

                _logger.LogInformation("Событие task.completed отправлено для задачи {TaskId}", evt.TaskId);
            }
            catch (Exception ex)
            {
                // RabbitMQ недоступен -- логируем, но не падаем
                _logger.LogError(ex, "Не удалось опубликовать событие для задачи {TaskId}", evt.TaskId);
                _rabbitChannel = null;
            }
        }
    }

    private async Task ConnectAsync(CancellationToken ct)
    {
        try
        {
            var factory = new ConnectionFactory
            {
                HostName = _config["RabbitMq:Host"] ?? "localhost",
                Port = int.TryParse(_config["RabbitMq:Port"], out var p) ? p : 5672,
                UserName = _config["RabbitMq:User"] ?? "guest",
                Password = _config["RabbitMq:Password"] ?? "guest"
            };

            _connection = await factory.CreateConnectionAsync(ct);
            _rabbitChannel = await _connection.CreateChannelAsync(cancellationToken: ct);

            // Объявляем exchange при подключении
            await _rabbitChannel.ExchangeDeclareAsync(
                exchange: Exchange,
                type: ExchangeType.Direct,
                durable: true,
                autoDelete: false,
                cancellationToken: ct);

            _logger.LogInformation("Подключились к RabbitMQ, exchange {Exchange} объявлен", Exchange);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Не удалось подключиться к RabbitMQ");
        }
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        _channel.Writer.Complete();

        // Даём 5 секунд на допубликацию оставшихся сообщений
        var drainTask = base.StopAsync(cancellationToken);
        var timeout = Task.Delay(TimeSpan.FromSeconds(5), CancellationToken.None);
        await Task.WhenAny(drainTask, timeout);

        try
        {
            if (_rabbitChannel != null)
                await _rabbitChannel.CloseAsync();
            if (_connection != null)
                await _connection.CloseAsync();
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Ошибка при закрытии соединения с RabbitMQ");
        }
    }
}
