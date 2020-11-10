using System.Collections.Generic;
using System.Threading.Tasks;

namespace AnalyticalWays.Utilities.Interfaces {
    public interface ISQLProcessing {
        // Verifica si existen datos previos a eliminar
        Task<bool> ExistsPreviousData();

        // Borra los datos anteriores
        Task<bool> DeletePreviousData();

        // Inserta datos en la base de datos
        Task<bool> AppendData<TData>(IEnumerable<TData> datos);
    }
}
