using Microsoft.EntityFrameworkCore;

namespace TaskApi;

public class TaskDbContext : DbContext
{
    public TaskDbContext(DbContextOptions<TaskDbContext> options) : base(options) { }

    public DbSet<TaskItem> Tasks => Set<TaskItem>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<TaskItem>(e =>
        {
            e.HasKey(t => t.Id);
            e.Property(t => t.Title).HasMaxLength(200).IsRequired();
            e.Property(t => t.Priority).HasConversion<string>();

            if (Database.IsNpgsql())
            {
                e.Property(t => t.Version).IsRowVersion();
            }
            else
            {
                e.Ignore(t => t.Version);
            }
        });
    }
}
