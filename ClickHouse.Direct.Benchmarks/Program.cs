using System.Reflection;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Diagnosers;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Running;
using Spectre.Console;

namespace ClickHouse.Direct.Benchmarks;

internal class Program
{
    private static void Main(string[] args)
    {
        // Check for connection string argument first
        for (var i = 0; i < args.Length - 1; i++)
        {
            if (args[i] != "--connection" && args[i] != "-c")
                continue;

            Environment.SetEnvironmentVariable("CLICKHOUSE_CONNECTION_STRING", args[i + 1]);
            args = args.Take(i).Concat(args.Skip(i + 2)).ToArray();
            break;
        }
        
        // If BenchmarkDotNet's own arguments are passed, use its runner directly
        // This prevents the discovery output from appearing after benchmark results
        if (args.Any(arg => arg.StartsWith("--filter") || arg.StartsWith("--list") || 
                            arg.StartsWith("--join") || arg.StartsWith("--all") ||
                            arg.StartsWith("--anyCategories") || arg.StartsWith("--allCategories")))
        {
            BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args);
            return;
        }

        AnsiConsole.Write(
            new FigletText("Benchmarks")
                .LeftJustified()
                .Color(Color.Blue));

        AnsiConsole.MarkupLine("[bold yellow]Reflection-based Benchmark Runner[/]");
        AnsiConsole.WriteLine();

        if (args.Length > 0)
        {
            RunCommandLine(args);
            return;
        }

        if (!AnsiConsole.Profile.Capabilities.Interactive)
        {
            ShowHelp();
            return;
        }

        RunInteractive();
    }

    private static void ShowHelp()
    {
        AnsiConsole.MarkupLine("[yellow]Usage:[/]");
        AnsiConsole.MarkupLine("  dotnet run                                   - Interactive mode");
        AnsiConsole.MarkupLine("  dotnet run -- --list                         - List all benchmarks");
        AnsiConsole.MarkupLine("  dotnet run -- --all                          - Run all benchmarks");
        AnsiConsole.MarkupLine("  dotnet run -- --filter <text>                - Run benchmarks matching filter");
        AnsiConsole.MarkupLine("  dotnet run -- --class <name>                 - Run specific benchmark class");
        AnsiConsole.MarkupLine("  dotnet run -- --info                         - Show system information");
        AnsiConsole.MarkupLine("  dotnet run -- --connection <string> [args]   - Set ClickHouse connection string");
        AnsiConsole.MarkupLine("  dotnet run -- -c <string> [args]             - Set ClickHouse connection string (short)");
        AnsiConsole.WriteLine();
        AnsiConsole.MarkupLine("[yellow]Examples:[/]");
        AnsiConsole.MarkupLine("  dotnet run -- --connection \"Host=localhost;Port=9000\" --filter ClickHouseVsDuckDB");
        AnsiConsole.MarkupLine("  dotnet run -- -c \"Host=myserver;Port=9000;User=admin\" --all");
    }

    private static void RunCommandLine(IReadOnlyList<string> args)
    {
        var command = args[0].ToLowerInvariant();
        
        switch (command)
        {
            case "--list":
                ListBenchmarks();
                break;
            case "--all":
                RunAllBenchmarks();
                break;
            case "--filter" when args.Count > 1:
                RunFiltered(args[1]);
                break;
            case "--class" when args.Count > 1:
                RunClass(args[1]);
                break;
            case "--info":
                ShowSystemInfo();
                break;
            default:
                ShowHelp();
                break;
        }
    }

    private static void RunInteractive()
    {
        while (true)
        {
            AnsiConsole.Clear();
            
            var choice = AnsiConsole.Prompt(
                new SelectionPrompt<string>()
                    .Title("[yellow]Select an option:[/]")
                    .AddChoices([
                        "Browse Benchmarks",
                        "Run Specific Benchmark",
                        "Run All Benchmarks",
                        "Custom Configuration",
                        "System Information",
                        "Exit"
                    ]));

            switch (choice)
            {
                case "Browse Benchmarks":
                    BrowseBenchmarks();
                    break;
                case "Run Specific Benchmark":
                    RunSpecificBenchmark();
                    break;
                case "Run All Benchmarks":
                    RunAllBenchmarks();
                    break;
                case "Custom Configuration":
                    RunWithCustomConfig();
                    break;
                case "System Information":
                    ShowSystemInfo();
                    break;
                case "Exit":
                    return;
            }

            if (choice != "Exit")
            {
                AnsiConsole.WriteLine();
                AnsiConsole.MarkupLine("[dim]Press any key to continue...[/]");
                Console.ReadKey(true);
            }
        }
    }

    private static void BrowseBenchmarks()
    {
        var benchmarks = DiscoverBenchmarks();
        
        if (benchmarks.Count == 0)
        {
            AnsiConsole.MarkupLine("[red]No benchmarks found in assembly[/]");
            return;
        }

        foreach (var benchmark in benchmarks)
        {
            var tree = new Tree($"[yellow]{benchmark.Type.Name}[/]");
            
            if (benchmark.Methods.Count > 0)
            {
                var methodsNode = tree.AddNode($"[green]Methods ({benchmark.Methods.Count})[/]");
                foreach (var method in benchmark.Methods)
                {
                    var label = method.IsBaseline ? $"{method.Name} [dim](baseline)[/]" : method.Name;
                    methodsNode.AddNode(label);
                }
            }

            if (benchmark.Parameters.Count > 0)
            {
                var paramsNode = tree.AddNode("[blue]Parameters[/]");
                foreach (var param in benchmark.Parameters)
                {
                    var values = string.Join(", ", param.Values.Select(v => v?.ToString() ?? "null"));
                    var escapedValues = values.Replace("[", "[[").Replace("]", "]]");
                    paramsNode.AddNode($"{param.Name} ({param.Type.Name}): [[{escapedValues}]]");
                }
            }

            AnsiConsole.Write(tree);
            AnsiConsole.WriteLine();
        }
    }

    private static void ListBenchmarks()
    {
        var benchmarks = DiscoverBenchmarks();
        
        if (benchmarks.Count == 0)
        {
            AnsiConsole.MarkupLine("[red]No benchmarks found[/]");
            return;
        }

        AnsiConsole.MarkupLine($"[yellow]Found {benchmarks.Count} benchmark class(es):[/]");
        foreach (var benchmark in benchmarks)
        {
            AnsiConsole.MarkupLine($"  [green]{benchmark.Type.Name}[/] - {benchmark.Methods.Count} methods");
        }
    }

    private static void RunSpecificBenchmark()
    {
        var benchmarks = DiscoverBenchmarks();
        
        if (benchmarks.Count == 0)
        {
            AnsiConsole.MarkupLine("[red]No benchmarks found[/]");
            return;
        }

        var selected = AnsiConsole.Prompt(
            new SelectionPrompt<BenchmarkInfo>()
                .Title("[yellow]Select benchmark:[/]")
                .AddChoices(benchmarks)
                .UseConverter(b => $"{b.Type.Name} ({b.Methods.Count} methods)"));

        var config = AskForConfig();
        
        AnsiConsole.MarkupLine($"[green]Running {selected.Type.Name}...[/]");
        BenchmarkRunner.Run(selected.Type, config);
    }

    private static void RunAllBenchmarks()
    {
        var benchmarks = DiscoverBenchmarks();
        
        if (benchmarks.Count == 0)
        {
            AnsiConsole.MarkupLine("[red]No benchmarks found[/]");
            return;
        }

        var config = DefaultConfig.Instance;
        
        foreach (var benchmark in benchmarks)
        {
            AnsiConsole.MarkupLine($"[green]Running {benchmark.Type.Name}...[/]");
            BenchmarkRunner.Run(benchmark.Type, config);
        }
    }

    private static void RunFiltered(string filter)
    {
        var benchmarks = DiscoverBenchmarks()
            .Where(b => b.Type.Name.Contains(filter, StringComparison.OrdinalIgnoreCase))
            .ToList();

        if (benchmarks.Count == 0)
        {
            AnsiConsole.MarkupLine($"[red]No benchmarks matching '{filter}'[/]");
            return;
        }

        foreach (var benchmark in benchmarks)
        {
            AnsiConsole.MarkupLine($"[green]Running {benchmark.Type.Name}...[/]");
            BenchmarkRunner.Run(benchmark.Type);
        }
    }

    private static void RunClass(string className)
    {
        var benchmarks = DiscoverBenchmarks();
        var benchmark = benchmarks.FirstOrDefault(b => 
            b.Type.Name.Equals(className, StringComparison.OrdinalIgnoreCase));

        if (benchmark == null)
        {
            AnsiConsole.MarkupLine($"[red]Benchmark class '{className}' not found[/]");
            return;
        }

        AnsiConsole.MarkupLine($"[green]Running {benchmark.Type.Name}...[/]");
        BenchmarkRunner.Run(benchmark.Type);
    }

    private static void RunWithCustomConfig()
    {
        var benchmarks = DiscoverBenchmarks();
        
        if (benchmarks.Count == 0)
        {
            AnsiConsole.MarkupLine("[red]No benchmarks found[/]");
            return;
        }

        var selected = AnsiConsole.Prompt(
            new MultiSelectionPrompt<BenchmarkInfo>()
                .Title("[yellow]Select benchmarks to run:[/]")
                .AddChoices(benchmarks)
                .UseConverter(b => $"{b.Type.Name} ({b.Methods.Count} methods)"));

        var config = BuildCustomConfig();

        foreach (var benchmark in selected)
        {
            AnsiConsole.MarkupLine($"[green]Running {benchmark.Type.Name}...[/]");
            BenchmarkRunner.Run(benchmark.Type, config);
        }
    }

    private static IConfig AskForConfig()
    {
        var useDefault = AnsiConsole.Confirm("Use default configuration?");
        return useDefault ? DefaultConfig.Instance : BuildCustomConfig();
    }

    private static IConfig BuildCustomConfig()
    {
        var config = ManualConfig.CreateEmpty();

        var jobChoice = AnsiConsole.Prompt(
            new SelectionPrompt<string>()
                .Title("[yellow]Select job configuration:[/]")
                .AddChoices([
                    "Quick (1 warmup, 3 iterations)",
                    "Default (3 warmups, 10 iterations)", 
                    "Thorough (5 warmups, 20 iterations)",
                    "Custom"
                ]));

        var job = jobChoice switch
        {
            "Quick (1 warmup, 3 iterations)" => 
                Job.Default.WithWarmupCount(1).WithIterationCount(3),
            "Thorough (5 warmups, 20 iterations)" => 
                Job.Default.WithWarmupCount(5).WithIterationCount(20),
            "Custom" => 
                Job.Default
                    .WithWarmupCount(AnsiConsole.Ask("Warmup count:", 3))
                    .WithIterationCount(AnsiConsole.Ask("Iteration count:", 10)),
            _ => 
                Job.Default
        };

        config = config.AddJob(job);

        if (AnsiConsole.Confirm("Include memory diagnostics?"))
        {
            config = config.AddDiagnoser(MemoryDiagnoser.Default);
        }

        if (AnsiConsole.Confirm("Include disassembly diagnostics?"))
        {
            var depth = AnsiConsole.Ask("Max disassembly depth:", 3);
            config = config.AddDiagnoser(new DisassemblyDiagnoser(
                new DisassemblyDiagnoserConfig(maxDepth: depth)));
        }

        return config;
    }

    private static void ShowSystemInfo()
    {
        var table = new Table();
        table.AddColumn("Property");
        table.AddColumn("Value");

        table.AddRow("OS", System.Runtime.InteropServices.RuntimeInformation.OSDescription);
        table.AddRow("Architecture", System.Runtime.InteropServices.RuntimeInformation.ProcessArchitecture.ToString());
        table.AddRow(".NET Version", System.Runtime.InteropServices.RuntimeInformation.FrameworkDescription);
        table.AddRow("Processor Count", Environment.ProcessorCount.ToString());

        // Check for common SIMD support
        var simdTypes = new[]
        {
            ("AVX512F", "System.Runtime.Intrinsics.X86.Avx512F"),
            ("AVX2", "System.Runtime.Intrinsics.X86.Avx2"),
            ("AVX", "System.Runtime.Intrinsics.X86.Avx"),
            ("SSE2", "System.Runtime.Intrinsics.X86.Sse2"),
            ("ARM AdvSIMD", "System.Runtime.Intrinsics.Arm.AdvSimd")
        };

        table.AddRow(new Text(""), new Text(""));
        table.AddRow("[bold]SIMD Support[/]", "");

        foreach (var (name, typeName) in simdTypes)
        {
            var type = Type.GetType(typeName);
            if (type != null)
            {
                var isSupported = type.GetProperty("IsSupported")?.GetValue(null) as bool? ?? false;
                table.AddRow(name, isSupported ? "[green]✓[/]" : "[red]✗[/]");
            }
        }

        AnsiConsole.Write(table);
    }

    private static List<BenchmarkInfo> DiscoverBenchmarks()
    {
        var assembly = Assembly.GetExecutingAssembly();
        var benchmarks = new List<BenchmarkInfo>();

        foreach (var type in assembly.GetTypes())
        {
            if (!type.IsClass || type.IsAbstract || type.IsNested)
                continue;

            var methods = type.GetMethods(BindingFlags.Public | BindingFlags.Instance)
                .Where(m => m.GetCustomAttribute<BenchmarkAttribute>() != null)
                .Select(m => new MethodInfo
                {
                    Name = m.Name,
                    IsBaseline = m.GetCustomAttribute<BenchmarkAttribute>()?.Baseline ?? false
                })
                .ToList();

            if (methods.Count == 0)
                continue;

            var parameters = new List<ParameterInfo>();
            foreach (var property in type.GetProperties(BindingFlags.Public | BindingFlags.Instance))
            {
                var paramsAttr = property.GetCustomAttribute<ParamsAttribute>();
                if (paramsAttr != null)
                {
                    parameters.Add(new ParameterInfo
                    {
                        Name = property.Name,
                        Type = property.PropertyType,
                        Values = paramsAttr.Values
                    });
                }
            }

            benchmarks.Add(new BenchmarkInfo
            {
                Type = type,
                Methods = methods,
                Parameters = parameters
            });
        }

        return benchmarks.OrderBy(b => b.Type.Name).ToList();
    }

    private class BenchmarkInfo
    {
        public Type Type { get; init; } = null!;
        public List<MethodInfo> Methods { get; init; } = [];
        public List<ParameterInfo> Parameters { get; init; } = [];
    }

    private class MethodInfo
    {
        public string Name { get; init; } = "";
        public bool IsBaseline { get; init; }
    }

    private class ParameterInfo
    {
        public string Name { get; init; } = "";
        public Type Type { get; init; } = null!;
        public object?[] Values { get; init; } = Array.Empty<object?>();
    }
}