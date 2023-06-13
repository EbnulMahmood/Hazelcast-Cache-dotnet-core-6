namespace Document
{
    public readonly record struct Customer(int Id, string Name, string Address, DateTimeOffset CreatedAt);
}
