namespace AnalyticalWays.DataProcessor.Configuration {
    /// <summary>
    /// Configuración para el procesador de archivo CSV
    /// </summary>
    public class CsvProcessorConfiguration {
        /// <summary>
        /// Configuración de Azure Blob Storage
        /// </summary>
        public BlobStorageConfiguration BlobStorageConfiguration { get; set; }
        /// <summary>
        /// Configuración de SQL Server
        /// </summary>
        public SQLProcessingConfiguration SQLProcessingConfiguration { get; set; }
        /// <summary>
        /// Configuración de seguimiento de errores
        /// </summary>
        public TrackingConfiguration TrackingConfiguration { get; set; }
    }
}
