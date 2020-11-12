namespace AnalyticalWays.DataProcessor.Configuration {
    /// <summary>
    /// Configuración de procesamiento de SQL
    /// </summary>
    public class SQLProcessingConfiguration {
        /// <summary>
        /// Cantidad máxima de tareas a ejecutar para el procesamiento de datos
        /// </summary>
        public int MaxTasks { get; set; }
        /// <summary>
        /// Tamaño máximo del batch de datos a procesar
        /// </summary>
        public int BatchSize { get; set; }
        /// <summary>
        /// Determina si el procesamiento de SQL debe detenerse si ocurre un error
        /// </summary>
        public bool AbortOnError { get; set; }
    }
}
