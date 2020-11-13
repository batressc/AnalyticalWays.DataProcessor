using AnalyticalWays.DataProcessor.Configuration;
using AnalyticalWays.DataProcessor.Contracts;
using AnalyticalWays.DataProcessor.Model;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace AnalyticalWays.DataProcessor {
    /// <summary>
    /// Realiza el proceso de lectura del archivo CSV en el Storage y lo almacena en un Channel para su consumo
    /// </summary>
    public class Producer {
        private readonly IStorageOperations _storage;
        private readonly CsvProcessorConfiguration _conf;
        private readonly ILogger<Producer> _logger;

        /// <summary>
        /// Crea una nueva instancia de <see cref="Producer"/>
        /// </summary>
        /// <param name="storage">Servicio de operaciones con el storage</param>
        /// <param name="conf">Configuración de la aplicación</param>
        /// <param name="logger">Servicio de logger</param>
        public Producer(IStorageOperations storage, IOptions<CsvProcessorConfiguration> conf, ILogger<Producer> logger) {
            _storage = storage;
            _conf = conf.Value;
            _logger = logger;
        }

        /// <summary>
        /// Crear los archivos de seguimiento de registros erroneos
        /// </summary>
        /// <param name="datos">Datos de registros erróneos</param>
        /// <param name="cancellationToken">Token de cancelación</param>
        private async Task CrearLogRegistrosErroneos(List<(string linea, int registro, string mensaje)> datos, CancellationToken cancellationToken) {
            _logger.LogWarning($"Se encontraron {datos.Count} registros erroneos. Generando archivo(s) de seguimiento...");
            try {
                List<string> registros = new List<string>() { string.Join(_conf.TrackingConfiguration.Separator, "registro", "error", "pos", "product", "date", "stock") };
                registros.AddRange(datos.Select(x => string.Join(_conf.TrackingConfiguration.Separator, x.registro, x.mensaje, x.linea)));
                await File.AppendAllLinesAsync(_conf.TrackingConfiguration.FileName, registros, Encoding.UTF8, cancellationToken);
                _logger.LogInformation("Archivo de seguimiento de registros erroneos generado exitosamente");
            } catch (Exception ex) {
                _ = ex;
                _logger.LogError("No se pudo generar el archivo de seguimiento de registros erroneos");
            }
        }

        /// <summary>
        /// Transforma la cadena de datos separada por punto y coma a un objeto del tipo StockInformation
        /// </summary>
        /// <param name="datos">Cadena a transformar</param>
        /// <returns>Objeto del tipo <see cref="StockInformation"/> o null si no es posible transformarlo</returns>
        private (StockInformation stock, string mensaje) StringToStockInformation(string datos) {
            StockInformation resultado = null;
            string mensaje = "";
            if (!string.IsNullOrWhiteSpace(datos)) {
                string[] elementos = datos.Split(";");
                // Verificamos que los componentes de la cadena sean 4
                if (elementos.Length == 4) {
                    // Validando que la fecha posea el formato año-mes-dia
                    DateTime.TryParseExact(elementos[2], "yyyy-MM-dd", CultureInfo.CurrentCulture, DateTimeStyles.None, out DateTime fechaStock);
                    // Si todo está correcto, creamos el objeto, caso contrario devolvemos null
                    if (fechaStock != null) {
                        resultado = new StockInformation() {
                            Pos = elementos[0],
                            Product = elementos[1],
                            StockDate = fechaStock,
                            Stock = Convert.ToInt32(elementos[3])
                        };
                    } else {
                        mensaje = $"La fecha del registro posee un formato incorrecto";
                    }
                } else {
                    mensaje = $"La cantidad de elementos del registro ({elementos.Length}) no es acorde al formato esperado";
                }
            }
            return (resultado, mensaje);
        }

        /// <summary>
        /// Realiza la lectura del archivo en Azure Blob Storage y almacena la información en un Channel para su procesamiento
        /// </summary>
        /// <param name="filename">Ruta del archivo</param>
        /// <param name="channel">Channel utilizado para la gestión de la información</param>
        /// <param name="cancellationToken">Token de cancelación</param>
        /// <param name="cts">Cancelation token source utilizado para detener los procesos paralelos</param>
        /// <returns>Tarea de recuperación y preparación de datos</returns>
        public async Task<TimeSpan> PrepararDatos(string filename, Channel<StockInformation> channel, CancellationToken cancellationToken, CancellationTokenSource cts) {
            List<(string linea, int registro, string mensaje)> registrosErroneos = new List<(string linea, int registro, string mensaje)>();
            int registro = 0;
            _logger.LogInformation($"Iniciando lectura de archivo \"{_conf.BlobStorageConfiguration.FileName}\"...");
            Stopwatch cronometro = new Stopwatch();
            cronometro.Start();
            try {
                // Recuperando stream desde el storage
                Stream stream = await _storage.ReadFile(filename, cancellationToken);
                using StreamReader reader = new StreamReader(stream);
                bool primeraLinea = true;
                // Iterando sobre el stream
                while (!reader.EndOfStream) {
                    registro++;
                    string linea = await reader.ReadLineAsync();
                    if (primeraLinea) {
                        primeraLinea = false;
                        continue;
                    }
                    // Transformando cadena a datos de Stock
                    (StockInformation info, string mensaje) = StringToStockInformation(linea);
                    if (info != null) {
                        // Agregando inforamción al Channel para procesamiento por consumidores
                        await channel.Writer.WriteAsync(info, cancellationToken);
                    } else {
                        // Agregando registro erroneo para log de seguimiento de errores
                        registrosErroneos.Add((linea, registro, mensaje));
                    }
                }
                // Indicamos al Channel que ya no vamos a enviar más datos
                channel.Writer.Complete();
            } catch (Exception ex) {
                _logger.LogError(ex, $"Ha ocurrido un error inesperado durante la lectura del archivo{(_conf.SQLProcessingConfiguration.AbortOnError ? ". Se detiene el procesamiento de datos" : "")}");
                if (_conf.BlobStorageConfiguration.AbortOnError) {
                    cts.Cancel();
                }
            }
            // Preparando archivo de registros erroneos
            if (registrosErroneos.Count > 0) {
                await CrearLogRegistrosErroneos(registrosErroneos, cancellationToken);
            }
            // Deteniendo contador de tiempo y enviando información de tiempo de ejecución
            cronometro.Stop();
            _logger.LogInformation("Finalizada lectura de archivo");
            return cronometro.Elapsed;
        }
    }
}
