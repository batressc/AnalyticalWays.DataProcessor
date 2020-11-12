using AnalyticalWays.DataProcessor.Configuration;
using AnalyticalWays.DataProcessor.Contracts;
using AnalyticalWays.DataProcessor.Model;
using Microsoft.Extensions.Hosting;
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
    /// Proceso que permite almacenar la información de un archivo CSV en Azure Blob Storage a una base de datos local SQL Server.
    /// Se utiliza un patrón "Producer-Consumer" para optimizar el procesamiento de los datos
    /// </summary>
    public class CsvProcessor : BackgroundService {
        private readonly CsvProcessorConfiguration _conf;
        private readonly IStorageOperations _storage;
        private readonly ILogger<CsvProcessor> _logger;
        private readonly IDataOperations<StockInformation> _data;

        /// <summary>
        /// Crea una instancia de la clase <see cref="CsvProcessor"/>
        /// </summary>
        /// <param name="storage">Servicio de almacenamiento de archivos</param>
        /// <param name="data">Servicio de acceso a datos (SQL)</param>
        /// <param name="logger">Servicio de logging</param>
        public CsvProcessor(IOptions<CsvProcessorConfiguration> conf, IStorageOperations storage, IDataOperations<StockInformation> data, ILogger<CsvProcessor> logger) {
            _conf = conf.Value;
            _storage = storage;
            _data = data;
            _logger = logger;
        }

        /// <summary>
        /// Crear los archivos de seguimiento de registros erroneos
        /// </summary>
        /// <param name="datos">Datos de registros erróneos</param>
        private async Task CrearLogRegistrosErroneos(List<(string linea, int registro, string mensaje)> datos, CancellationToken cancellationToken) {
            _logger.LogWarning($"Se encontraron {datos.Count} registros erroneos. Generando archivo(s) de seguimiento...");
            try {
                List<string> registros = new List<string>() { string.Join(_conf.TrackingConfiguration.Separator, "registro","error","pos","product","date","stock") };
                registros.AddRange(datos.Select(x => string.Join(_conf.TrackingConfiguration.Separator, x.registro, x.mensaje, x.linea)));
                await File.AppendAllLinesAsync(_conf.TrackingConfiguration.FileName, registros, Encoding.UTF8, cancellationToken);
                _logger.LogInformation("Archivo de seguimiento de registros erroneos generado exitosamente");
            } catch (Exception ex) {
                _ = ex;
                _logger.LogError("No se pudo generar el archivo de seguimiento de registros erroneos");
            }
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
        private async Task<TimeSpan> PrepararDatos(string filename, Channel<StockInformation> channel, CancellationToken cancellationToken, CancellationTokenSource cts) {
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

        /// <summary>
        /// Realiza la inserción de datos a SQL Server (refactorización)
        /// </summary>
        /// <param name="cronometro">Stopwatch para control de tiempos</param>
        /// <param name="batchStock">Listado de elementos a insertar</param>
        /// <param name="tiempoGuardado">Total de tiempo de las operaciones de almacenamiento a SQL Server</param>
        /// <param name="proceso">Identificador del proceso</param>
        /// <param name="totalElementos">Total de elementos insertados</param>
        /// <param name="cancellationToken">Token de cancelación</param>
        /// <param name="cts">Cancelation token source utilizado para detener los procesos paralelos</param>
        private async Task AgregarDatos(Stopwatch cronometro, List<StockInformation> batchStock, TimeSpan tiempoGuardado, int proceso, int totalElementos, CancellationToken cancellationToken, CancellationTokenSource cts) {
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
            _logger.LogDebug($"Total de registros escritos por proceso #{proceso}: {(fueError ? 0 : totalElementos)} | Tiempo: {cronometro.Elapsed.Hours:00}:{cronometro.Elapsed.Minutes:00}:{cronometro.Elapsed.Seconds:00}.{cronometro.Elapsed.Milliseconds:000}/{tiempoGuardado.Hours:00}:{tiempoGuardado.Minutes:00}:{tiempoGuardado.Seconds:00}.{tiempoGuardado.Milliseconds:000}");
            cronometro.Reset();
        }

        /// <summary>
        /// Lee la información del Channel y la almacena en SQL Server
        /// </summary>
        /// <param name="channel">Channel utilizado para la gestión de la información</param>
        /// <param name="cancellationToken">Token de cancelación</param>
        /// <returns>Tarea de lectura y almacenamiento de datos</returns>
        private async Task<TimeSpan> GuardarDatos(int proceso, Channel<StockInformation> channel, CancellationToken cancellationToken, CancellationTokenSource cts) {
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
                    await AgregarDatos(cronometro, batchStock, tiempoGuardado, proceso, totalElementos, cancellationToken, cts);
                    elementosBatch = 0;
                }
            }
            // Agregando datos remanentes 
            if (batchStock.Count != 0) {
                await AgregarDatos(cronometro, batchStock, tiempoGuardado, proceso, totalElementos, cancellationToken, cts);
            }
            _logger.LogDebug($"Finalizado proceso #{proceso}");
            return tiempoGuardado;
        }

        // Inicio de la aplicación
        /// <inheritdoc/>
        protected override async Task ExecuteAsync(CancellationToken stoppingToken) {
            try {
                // Determinando la cantidad máxima de tareas para procesamiento
                int tareas = Environment.ProcessorCount > 1 ? Environment.ProcessorCount - 1 : 1;
                tareas = _conf.SQLProcessingConfiguration.MaxTasks == 0 ? tareas : tareas > _conf.SQLProcessingConfiguration.MaxTasks ? _conf.SQLProcessingConfiguration.MaxTasks : tareas;
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
                        Task.Run(() => PrepararDatos(_conf.BlobStorageConfiguration.FileName, canal, ctoken, cts))
                    };
                    for (int i = 1; i <= tareas; i++) {
                        int proceso = i;
                        procesos.Add(Task.Run(() => GuardarDatos(proceso, canal, ctoken, cts)));
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
