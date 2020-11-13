using AnalyticalWays.DataProcessor.Configuration;
using AnalyticalWays.DataProcessor.Contracts;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Options;
using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace AnalyticalWays.DataProcessor.Implementations {
    /// <summary>
    /// Operaciones de acceso a blobs en contenedores de Azure Blob Storage
    /// </summary>
    public class AzureBlobStorageOperations : IStorageOperations {
        private readonly BlobServiceClient _bsc;
        private readonly BlobContainerClient _bcc;
        private readonly CsvProcessorConfiguration _conf;

        /// <summary>
        /// Crea una nueva instancia de la clase <see cref="AzureBlobStorageOperations"/>
        /// </summary>
        /// <param name="conf">Servicio de configuración de la aplicación</param>
        public AzureBlobStorageOperations(IOptions<CsvProcessorConfiguration> conf) {
            _conf = conf.Value;
            _bsc = new BlobServiceClient(_conf.BlobStorageConfiguration.ConnectionString);
            _bcc = _bsc.GetBlobContainerClient(_conf.BlobStorageConfiguration.Container);
        }

        // Determina si el archivo especificado existe en el espacio de almacenamiento
        /// <inheritdoc/>
        public async Task<bool> FileExists(string filename, CancellationToken cancellationToken) {
            bool resultado = false;
            // Realizando la operación solo si existe el contenedor
            if (await _bcc.ExistsAsync(cancellationToken)) {
                BlobClient bc = _bcc.GetBlobClient(filename);
                resultado = await bc.ExistsAsync(cancellationToken);
            }
            return resultado;
        }

        // Obtiene la información del archivo especificado y permite aplicar las acciones a realizar con el Stream de datos del archivo
        /// <inheritdoc/>
        //public async Task ReadFile(string filename, Action<Stream> readAction, CancellationToken cancellationToken) {
        public async Task<Stream> ReadFile(string filename, CancellationToken cancellationToken) {
            BlobClient bc = _bcc.GetBlobClient(filename);
            // Realizando la lectura del archivo y ejecutando acción personalizada sobre el Stream resultante
            BlobOpenReadOptions options = new BlobOpenReadOptions(false) {
                BufferSize = _conf.BlobStorageConfiguration.DownloadBufferSizeMB * 1024 * 1024
            };
            Stream stream = await bc.OpenReadAsync(options, cancellationToken);
            //readAction.Invoke(stream);
            return stream;
        }
    }
}
