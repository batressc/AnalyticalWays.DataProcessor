namespace AnalyticalWays.DataProcessor.Configuration {
    /// <summary>
    /// Configuración para la lectura de los archivos desde Azure Blob Storage
    /// </summary>
    public class BlobStorageConfiguration {
        /// <summary>
        /// Cadena de conexión a la cuenta de almacenamiento
        /// </summary>
        public string ConnectionString { get; set; }
        /// <summary>
        /// Nombre del contenedor de blobs
        /// </summary>
        public string Container { get; set; }
        /// <summary>
        /// Archivo a procesar
        /// </summary>
        public string FileName { get; set; }
        /// <summary>
        /// Tamaño del buffer de descarga
        /// </summary>
        public int DownloadBufferSizeMB { get; set; }
        /// <summary>
        /// Determina si la lectura del archivo debe deternerse si ocurre un error
        /// </summary>
        public bool AbortOnError { get; set; }
    }
}
