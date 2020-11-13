using AnalyticalWays.DataProcessor.Configuration;
using AnalyticalWays.DataProcessor.Contracts;
using AnalyticalWays.DataProcessor.Model;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace AnalyticalWays.DataProcessor {
    /// <summary>
    /// Proceso que permite almacenar la información de un archivo CSV en Azure Blob Storage a una base de datos local SQL Server.
    /// Se utiliza un patrón "Producer-Consumer" para optimizar el procesamiento de los datos
    /// </summary>
    public class CsvProcessor : BackgroundService {
        private readonly CsvProcessorConfiguration _conf;
        private readonly IStorageOperations _storage;
        private readonly ILogger<CsvProcessor> _logger;
        private readonly IDataOperations<StockInformation> _data;
        private readonly Producer _consumer;
        private readonly Consumer _producer;

        /// <summary>
        /// Crea una instancia de la clase <see cref="CsvProcessor"/>
        /// </summary>
        /// <param name="consumer">Servicio consumidor</param>
        /// <param name="producer">Servicio productor</param>
        /// <param name="conf">Configuración de la aplicación</param>
        /// <param name="storage">Servicio de almacenamiento de archivos</param>
        /// <param name="data">Servicio de acceso a datos (SQL)</param>
        /// <param name="logger">Servicio de logging</param>
        public CsvProcessor(Producer consumer, Consumer producer, IOptions<CsvProcessorConfiguration> conf, IStorageOperations storage, IDataOperations<StockInformation> data, ILogger<CsvProcessor> logger) {
            _consumer = consumer;
            _producer = producer;
            _conf = conf.Value;
            _storage = storage;
            _data = data;
            _logger = logger;
        }

        /// <summary>
        /// Muestra los resultados de los tiempos de ejecución totales
        /// </summary>
        /// <param name="tiempos">Tiempos de ejecución</param>
        private void MostrarTiempos(TimeSpan[] tiempos) {
            TimeSpan tiempoProcesos = new TimeSpan();
            _logger.LogInformation($"Tiempo total de descarga de archivo: {tiempos[0].Hours:00}:{tiempos[0].Minutes:00}:{tiempos[0].Seconds:00}.{tiempos[0].Milliseconds:000}");
            for (int i = 1; i < tiempos.Length; i++) {
                _logger.LogInformation($"Tiempo de almacenamiento de tarea {(tiempos.Length > 2 ? "paralela" : "")} #{i}: {tiempos[0].Hours:00}:{tiempos[i].Minutes:00}:{tiempos[i].Seconds:00}.{tiempos[i].Milliseconds:000}");
                tiempoProcesos += tiempos[i];
            }
            // Mostrando tiempo total de varios procesos. Si es > 2 implica que hubo un proceso de lectura y más de uno de escritura
            if (tiempos.Length > 2) {
                TimeSpan tiempoMaximo = tiempos.Skip(1).Max(x => x);
                _logger.LogInformation($"Tiempo de almacenamiento máximo: {tiempos[0].Hours:00}:{tiempoMaximo.Minutes:00}:{tiempoMaximo.Seconds:00}.{tiempoMaximo.Milliseconds:000}");
            }
        }

        /// <summary>
        /// Especifica la cantidad de tares consumidoras a crear para el procesamiento de datos SQL
        /// </summary>
        /// <returns>Cantidad de tareas consumidoras</returns>
        private int CalcularTareasConsumidores() {
            int tareas = Environment.ProcessorCount > 1 ? Environment.ProcessorCount - 1 : 1;
            if (_conf.SQLProcessingConfiguration.MaxTasks != 0) {
                if (tareas > _conf.SQLProcessingConfiguration.MaxTasks) {
                    tareas = _conf.SQLProcessingConfiguration.MaxTasks;
                }
            }
            return tareas;
        }

        // Inicio de la aplicación
        /// <inheritdoc/>
        protected override async Task ExecuteAsync(CancellationToken stoppingToken) {
            try {
                // Determinando la cantidad máxima de tareas para procesamiento
                int tareas = CalcularTareasConsumidores();
                // Lectura y procesamiento de datos
                Stopwatch cronometro = new Stopwatch();
                cronometro.Start();
                if (await _storage.FileExists(_conf.BlobStorageConfiguration.FileName, stoppingToken)) {
                    // Borrando datos anteriores si existieran
                    if (await _data.ExistsPreviousData(stoppingToken)) {
                        await _data.DeletePreviousData(stoppingToken);
                    }
                    // Creando Channel para gestión nativa de patrón productor-consumidor
                    Channel<StockInformation> canal = Channel.CreateUnbounded<StockInformation>();
                    // Creando token de cancelación para parada manual (basado en token del BackgroundService)
                    CancellationTokenSource cts = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
                    CancellationToken ctoken = cts.Token;
                    // Creando procesos
                    List<Task<TimeSpan>> procesos = new List<Task<TimeSpan>> {
                        Task.Run(() => _consumer.PrepararDatos(_conf.BlobStorageConfiguration.FileName, canal, ctoken, cts))
                    };
                    for (int i = 1; i <= tareas; i++) {
                        int proceso = i;
                        procesos.Add(Task.Run(() => _producer.GuardarDatos(proceso, canal, ctoken, cts)));
                    }
                    // Ejecutando procesos
                    TimeSpan[] procesamiento = await Task.WhenAll(procesos);
                    MostrarTiempos(procesamiento);
                }
                cronometro.Stop();
                _logger.LogInformation($"Tiempo total de ejecución: {cronometro.Elapsed.Hours:00}:{cronometro.Elapsed.Minutes:00}:{cronometro.Elapsed.Seconds:00}.{cronometro.Elapsed.Milliseconds:000}");
            } catch (Exception ex) {
                _logger.LogError(ex, "Ha ocurrido un error inesperado durante le procesamiento de la información. Ver más detalles en la excepción generada");
            }
        }
    }
}
