namespace ClickHouse.Direct.IntegrationTests;

/// <summary>
/// Test collection definition to share the ClickHouse container across all integration tests.
/// This ensures the container is started once per test run, not per test class.
/// </summary>
[CollectionDefinition("ClickHouse")]
public class ClickHouseCollection : ICollectionFixture<ClickHouseContainerFixture>;