using System.Net;
using System.Net.Http.Json;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using TaskApi;

namespace TaskApi.Tests;

// Заглушка вместо RabbitMQ -- складывает события в список
public class FakeEventPublisher : IEventPublisher
{
    public List<TaskCompletedEvent> Events { get; } = new();

    public ValueTask PublishAsync(TaskCompletedEvent evt)
    {
        Events.Add(evt);
        return ValueTask.CompletedTask;
    }
}

public class CompleteTaskIntegrationTest : IClassFixture<WebApplicationFactory<Program>>, IDisposable
{
    private readonly WebApplicationFactory<Program> _factory;
    private readonly FakeEventPublisher _fakePublisher = new();
    private readonly SqliteConnection _connection;

    public CompleteTaskIntegrationTest(WebApplicationFactory<Program> factory)
    {
        // Держим SQLite-соединение открытым, чтобы in-memory БД не пропадала
        _connection = new SqliteConnection("DataSource=:memory:");
        _connection.Open();

        _factory = factory.WithWebHostBuilder(builder =>
        {
            builder.ConfigureServices(services =>
            {
                // Выкидываем все регистрации DbContext и Npgsql
                var descriptors = services
                    .Where(d => d.ServiceType.FullName?.Contains("EntityFramework") == true
                             || d.ServiceType.FullName?.Contains("Npgsql") == true
                             || d.ServiceType == typeof(DbContextOptions<TaskDbContext>))
                    .ToList();
                foreach (var d in descriptors)
                    services.Remove(d);

                services.AddDbContext<TaskDbContext>(opt =>
                    opt.UseSqlite(_connection));

                // Подменяем publisher заглушкой
                services.RemoveAll<IEventPublisher>();
                services.AddSingleton<IEventPublisher>(_fakePublisher);

                // Убираем фоновый сервис RabbitMQ, он в тестах не нужен
                services.RemoveAll<IHostedService>();
            });
        });

        // Создаём схему
        using var scope = _factory.Services.CreateScope();
        var db = scope.ServiceProvider.GetRequiredService<TaskDbContext>();
        db.Database.EnsureCreated();
    }

    [Fact]
    public async Task CompleteTask_ReturnsOk_AndPublishesEvent()
    {
        var client = _factory.CreateClient();

        // 1. Создаём задачу
        var createReq = new { title = "Тестовая задача", priority = "High" };
        var createResp = await client.PostAsJsonAsync("/tasks", createReq);
        Assert.Equal(HttpStatusCode.Created, createResp.StatusCode);

        var created = await createResp.Content.ReadFromJsonAsync<TaskItemDto>();
        Assert.NotNull(created);
        Assert.Equal("Тестовая задача", created!.Title);
        Assert.False(created.IsCompleted);

        // 2. Завершаем задачу
        var completeResp = await client.PutAsync($"/tasks/{created.Id}/complete", null);
        Assert.Equal(HttpStatusCode.OK, completeResp.StatusCode);

        var completed = await completeResp.Content.ReadFromJsonAsync<TaskItemDto>();
        Assert.NotNull(completed);
        Assert.True(completed!.IsCompleted);
        Assert.NotNull(completed.CompletedAt);

        // 3. Проверяем, что событие ушло (в заглушку)
        Assert.Single(_fakePublisher.Events);
        var evt = _fakePublisher.Events[0];
        Assert.Equal(created.Id, evt.TaskId);
        Assert.Equal("Тестовая задача", evt.Title);
        Assert.Equal("High", evt.Priority);
        Assert.NotEqual(default, evt.CompletedAt);
    }

    public void Dispose()
    {
        _connection.Dispose();
    }

    // Минимальный DTO для десериализации ответов
    private record TaskItemDto(
        Guid Id,
        string Title,
        bool IsCompleted,
        string Priority,
        DateTime CreatedAt,
        DateTime? CompletedAt);
}
