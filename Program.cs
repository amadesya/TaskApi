using System.Text.Json.Serialization;
using System.Threading.Channels;
using Microsoft.EntityFrameworkCore;
using TaskApi;

#region Setup

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddDbContext<TaskDbContext>(opt =>
    opt.UseNpgsql(builder.Configuration.GetConnectionString("Postgres")));

builder.Services.ConfigureHttpJsonOptions(opt =>
{
    opt.SerializerOptions.Converters.Add(new JsonStringEnumConverter());
});

// Channel<T> как внутренняя очередь между эндпоинтом и фоновой публикацией
var eventChannel = Channel.CreateBounded<TaskCompletedEvent>(1000);
builder.Services.AddSingleton(eventChannel);
builder.Services.AddSingleton<IEventPublisher, RabbitMqPublisher>();
builder.Services.AddHostedService<RabbitMqBackground>();

var app = builder.Build();

#endregion

#region Endpoints

app.MapPost("/tasks", async (CreateTaskRequest? req, TaskDbContext db) =>
{
    // Валидация Title руками, без FluentValidation
    if (req is null || string.IsNullOrWhiteSpace(req.Title))
        return Results.BadRequest(new { error = "Title обязателен" });

    if (req.Title.Length > 200)
        return Results.BadRequest(new { error = "Title не должен превышать 200 символов" });

    var task = new TaskItem
    {
        Id = Guid.NewGuid(),
        Title = req.Title.Trim(),
        Priority = req.Priority ?? Priority.Medium,
        IsCompleted = false,
        CompletedAt = null,
        CreatedAt = DateTime.UtcNow
    };

    db.Tasks.Add(task);
    await db.SaveChangesAsync();

    return Results.Created($"/tasks/{task.Id}", task);
});

app.MapGet("/tasks", async (TaskDbContext db) =>
{
    // HACK: нет пагинации, на большом объёме данных так нельзя
    var tasks = await db.Tasks.ToListAsync();
    return Results.Ok(tasks);
});

app.MapPut("/tasks/{id:guid}/complete", async (Guid id, TaskDbContext db, IEventPublisher publisher) =>
{
    var task = await db.Tasks.FindAsync(id);
    if (task is null)
        return Results.NotFound(new { error = "Задача не найдена" });

    if (task.IsCompleted)
        return Results.Conflict(new { error = "Задача уже завершена" });

    task.IsCompleted = true;
    task.CompletedAt = DateTime.UtcNow;

    try
    {
        await db.SaveChangesAsync();
    }
    catch (DbUpdateConcurrencyException)
    {
        return Results.Conflict(new { error = "Конфликт параллельного обновления, попробуйте снова" });
    }

    // Публикуем ПОСЛЕ успешного SaveChanges
    // HACK: нет outbox-паттерна — если приложение упадёт между SaveChanges и публикацией, событие потеряется
    var evt = new TaskCompletedEvent(task.Id, task.Title, task.CompletedAt.Value, task.Priority.ToString());
    await publisher.PublishAsync(evt);

    return Results.Ok(task);
});

app.MapDelete("/tasks/{id:guid}", async (Guid id, TaskDbContext db) =>
{
    var task = await db.Tasks.FindAsync(id);
    if (task is null)
        return Results.NotFound(new { error = "Задача не найдена" });

    db.Tasks.Remove(task);
    await db.SaveChangesAsync();

    return Results.NoContent();
});

#endregion

#region Helpers

// Применяем миграции при старте (удобно для разработки)
using (var scope = app.Services.CreateScope())
{
    var db = scope.ServiceProvider.GetRequiredService<TaskDbContext>();
    // TODO: в проде не стоит мигрировать при старте
    if (db.Database.IsNpgsql())
        db.Database.Migrate();
    else
        db.Database.EnsureCreated(); // для тестов с SQLite
}

app.Run();

#endregion

// Нужно для WebApplicationFactory в тестах
public partial class Program { }
