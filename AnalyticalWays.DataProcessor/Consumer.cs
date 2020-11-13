using AnalyticalWays.DataProcessor.Configuration;
using AnalyticalWays.DataProcessor.Contracts;
using AnalyticalWays.DataProcessor.Model;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace AnalyticalWays.DataProcessor {
    /// <summary>
    /// Realiza el proceso de escritura de datos en SQL Server mediante Channel
    /// </summary>
    public class Consumer {
        private readonly CsvProcessorConfiguration _conf;
        private readonly IDataOperations<StockInformation> _data;
        private readonly ILogger<Consumer> _logger;

        /// <summary>
        /// Crea una nueva instancia de <see cref="Consumer"/>
        /// </summary>
        /// <param name="conf">Parámetros de configuración de la aplicación</param>
        /// <param name="data">Servicio de operaciones con SQL</param>
        /// <param name="logger">Servicio de logger</param>
        public Consumer(IOptions<CsvProcessorConfiguration> conf, IDataOperations<StockInformation> data, ILogger<Consumer> logger) {
            _conf = conf.Value;
            _data = data;
            _logger = logger;
        }

        /// <summary>
        /// Realiza la inserción de datos a SQL Server (refactorización)
        /// </summary>
        /// <param name="cronometro">Stopwatch para control de tiempos</param>
        /// <param name="batchStock">Listado de elementos a insertar</param>
        /// <param name="tiempoGuardado">Tiempo total de procesamiento</param>
        /// <param name="proceso">Identificador del proceso</param>
        /// <param name="totalElementos">Total de elementos insertados</param>
        /// <param name="cancellationToken">Token de cancelación</param>
        /// <param name="cts">Cancelation token source utilizado para detener los procesos paralelos</param>
        /// <returns>Tiempo total de inserción utilizado</returns>
        private async Task<TimeSpan> AgregarDatos(Stopwatch cronometro, List<StockInformation> batchStock, TimeSpan tiempoGuardado, int proceso, int totalElementos, CancellationToken cancellationToken, CancellationTokenSource cts) {
            bool fueError = false;
            cronometro.Start();
            try {
                await _data.AppendData(batchStock, cancellationToken);
            } catch (Exception ex) {
                fueError = true;
                _logger.LogError(ex, $"Ha ocurrido un error inesperado al guardar la información del proceso #{proceso}. {(_conf.SQLProcessingConfiguration.AbortOnError ? "Se detiene el procesamiento de datos" : "")}");
                // Si se especifica en la configuración, se detienen todos los subprocesos de procesamiento
                if (_conf.SQLProcessingConfiguration.AbortOnError) {
                    cts.Cancel();
                }
            }
            batchStock.Clear();
            cronometro.Stop();
            tiempoGuardado += cronometro.Elapsed;
            TimeSpan tiempoEjecucion = cronometro.Elapsed;
            _logger.LogDebug($"Total de registros escritos por proceso #{proceso}: {(fueError ? 0 : totalElementos)} | Tiempo: {cronometro.Elapsed.Hours:00}:{cronometro.Elapsed.Minutes:00}:{cronometro.Elapsed.Seconds:00}.{cronometro.Elapsed.Milliseconds:000}/{tiempoGuardado.Hours:00}:{tiempoGuardado.Minutes:00}:{tiempoGuardado.Seconds:00}.{tiempoGuardado.Milliseconds:000}");
            cronometro.Reset();
            return tiempoEjecucion;
        }

        /// <summary>
        /// Lee la información del Channel y la almacena en SQL Server
        /// </summary>
        /// <param name="proceso">Número del proceso</param>
        /// <param name="channel">Channel utilizado para la gestión de la información</param>
        /// <param name="cancellationToken">Token de cancelación</param>
        /// <param name="cts">CancellationTokenSource para detener las tareas de procesamiento</param>
        /// <returns>Tarea de lectura y almacenamiento de datos</returns>
        public async Task<TimeSpan> GuardarDatos(int proceso, Channel<StockInformation> channel, CancellationToken cancellationToken, CancellationTokenSource cts) {
            _logger.LogDebug($"Iniciando proceso #{proceso}");
            TimeSpan tiempoGuardado = new TimeSpan();
            Stopwatch cronometro = new Stopwatch();
            // Iniciando proceso de escritura en SQL Server
            int elementosBatch = 0;
            int totalElementos = 0;
            List<StockInformation> batchStock = new List<StockInformation>();
            // Leyendo información desde el Channel
            while (await channel.Reader.WaitToReadAsync(cancellationToken)) {
                StockInformation stockItem = await channel.Reader.ReadAsync(cancellationToken);
                batchStock.Add(stockItem);
                elementosBatch += 1;
                totalElementos += 1;
                if (elementosBatch >= _conf.SQLProcessingConfiguration.BatchSize) {
                    tiempoGuardado += await AgregarDatos(cronometro, batchStock, tiempoGuardado, proceso, totalElementos, cancellationToken, cts);
                    elementosBatch = 0;
                }
            }
            // Agregando datos remanentes 
            if (batchStock.Count != 0) {
                tiempoGuardado += await AgregarDatos(cronometro, batchStock, tiempoGuardado, proceso, totalElementos, cancellationToken, cts);
            }
            _logger.LogDebug($"Finalizado proceso #{proceso}");
            return tiempoGuardado;
        }
    }
}
