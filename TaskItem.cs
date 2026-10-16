using System.Text.Json.Serialization;

namespace TaskApi;

public enum Priority
{
    Low,
    Medium,
    High
}

public class TaskItem
{
    public Guid Id { get; set; }
    public string Title { get; set; } = string.Empty;
    public bool IsCompleted { get; set; }
    public Priority Priority { get; set; } = Priority.Medium;
    public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
    public DateTime? CompletedAt { get; set; }

    // xmin -- системный столбец Postgres, играет роль RowVersion.
    // Не нужно заводить своё поле и городить триггер, Postgres сам обновляет xmin при каждом UPDATE.
    // EF Core Npgsql умеет маппить через UseXminAsConcurrencyToken().
    public uint Version { get; set; }
}

// DTO для создания задачи (чтобы не тащить Id/CompletedAt в запрос)
public record CreateTaskRequest(string? Title, Priority? Priority);

// Событие для RabbitMQ
public record TaskCompletedEvent(Guid TaskId, string Title, DateTime CompletedAt, string Priority);
