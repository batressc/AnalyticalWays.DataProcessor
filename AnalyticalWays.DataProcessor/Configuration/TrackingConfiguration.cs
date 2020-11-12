namespace AnalyticalWays.DataProcessor.Configuration {
    /// <summary>
    /// Almacena los datos de configuración para seguimiento de errores
    /// </summary>
    public class TrackingConfiguration {
        /// <summary>
        /// Nombre del archivo de registros no procesados
        /// </summary>
        public string FileName { get; set; }
        /// <summary>
        /// Separador del archivo de registros no procesados
        /// </summary>
        public string Separator { get; set; }
    }
}
