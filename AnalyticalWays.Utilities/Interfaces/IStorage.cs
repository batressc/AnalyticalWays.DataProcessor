using System.Collections.Generic;
using System.Threading.Tasks;

namespace AnalyticalWays.Utilities.Interfaces {
    public interface IStorage {
        // Indica si el archivo especificado existe
        Task<bool> FileExists(string filename);

        // Obtiene los metadatos del archivo
        Task<TMedatada> GetMetadata<TMedatada>(string filename);

        // Lee los datos del archivo
        Task<bool> ReadFile(string filename);
    }
}
