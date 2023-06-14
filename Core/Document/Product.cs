namespace Document
{
    public readonly record struct Product(int Id, string Name, double Price, DateTimeOffset CreatedAt);
}
