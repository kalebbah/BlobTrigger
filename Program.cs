using Microsoft.Extensions.Hosting;
using Microsoft.Azure.Functions.Worker;

namespace FunctionTrigger
{
    public class Program
    {
        public static void Main()
        {
            var host = new HostBuilder()
                .ConfigureFunctionsWorkerDefaults()
                .Build();

            host.Run();
        }
    }
} 