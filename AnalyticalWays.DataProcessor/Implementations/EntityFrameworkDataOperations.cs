using AnalyticalWays.DataProcessor.Contracts;
using AnalyticalWays.DataProcessor.Model;
using EFCore.BulkExtensions;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace AnalyticalWays.DataProcessor.Implementations {
    public class EntityFrameworkDataOperations : IDataOperations<StockInformation> {
        private readonly IServiceScope _scope;
        private readonly AnalyticalWaysTestDbContext _db;

        public EntityFrameworkDataOperations(IServiceScopeFactory ssf) {
            _scope = ssf.CreateScope();
            _db = _scope.ServiceProvider.GetService<AnalyticalWaysTestDbContext>();
        }

        public async Task<bool> AppendData(IEnumerable<StockInformation> datos) {
            await _db.BulkInsertAsync(datos as List<StockInformation>);
            //await _db.StockInformation.AddRangeAsync(datos);
            int resultado = await _db.SaveChangesAsync();
            return resultado > 0;
        }

        public async Task<bool> DeletePreviousData() {
            int resultado = await _db.Database.ExecuteSqlRawAsync("truncate table [dbo].[StockInformation]");
            return resultado > 0;
        }

        public async Task<bool> ExistsPreviousData() {
            int resultado = await _db.StockInformation.CountAsync();
            return resultado > 0;
        }
    }
}
