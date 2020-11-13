using AnalyticalWays.DataProcessor.Configuration;
using AnalyticalWays.DataProcessor.Contracts;
using AnalyticalWays.DataProcessor.Implementations;
using AnalyticalWays.DataProcessor.Model;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace AnalyticalWays.DataProcessor {
    public class Startup {
        public Startup(IConfiguration configuration) {
            Configuration = configuration;
        }

        public IConfiguration Configuration { get; }

        public void ConfigureServices(IServiceCollection services) {
            // Mapeo de configuración a clase
            services.Configure<CsvProcessorConfiguration>(Configuration.GetSection("CsvProcessorConfiguration"));
            // Configuración de servicios
            // NOTA: Quitar comentarios entre las líneas 22 a 29 y comentar línea 30 si se desea probar mediante EF Core
            /*services.AddDbContext<AnalyticalWaysTestDbContext>(options => {
                options.UseSqlServer(Configuration.GetConnectionString("AnalyticalWaysDatabase"), opt => {
                    opt.EnableRetryOnFailure();
                });
                options.EnableDetailedErrors(true);
                options.EnableSensitiveDataLogging(true);
            });
            services.AddTransient<IDataOperations<StockInformation>, EntityFrameworkDataOperations>();*/
            services.AddTransient<IDataOperations<StockInformation>, ADODataOperations>();
            services.AddTransient<IStorageOperations, AzureBlobStorageOperations>();
            services.AddTransient<Producer>();
            services.AddTransient<Consumer>();
            // Proceso de tratamiento de archivo CSV
            services.AddHostedService<CsvProcessor>();
        }
    }
}
