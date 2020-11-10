using System.Threading.Tasks;

namespace AnalyticalWays.DataProcessor.Contracts {
    public interface IStorageOperations {
        Task<bool> FileExists(string filename);

        Task<bool> ReadFile(string filename);
    }
}
