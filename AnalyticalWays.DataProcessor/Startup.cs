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
            //services.AddDbContext<AnalyticalWaysTestDbContext>(options => {
            //    // Si hay problemas con transacciones, consultar este link
            //    // https://docs.microsoft.com/en-us/ef/core/miscellaneous/connection-resiliency
            //    options.UseSqlServer(Configuration.GetConnectionString("AnalyticalWaysDatabase"), opt => {
            //        opt.EnableRetryOnFailure();
            //    });
            //    options.EnableDetailedErrors(true);
            //    options.EnableSensitiveDataLogging(true);
            //});
            //services.AddTransient<IDataOperations<StockInformation>, EntityFrameworkDataOperations>();
            services.AddTransient<IDataOperations<StockInformation>, ADODataOperations>();
            services.AddTransient<IStorageOperations, AzureBlobStorageOperations>();

            services.AddHostedService<CsvProcessor>();
        }
    }
}
