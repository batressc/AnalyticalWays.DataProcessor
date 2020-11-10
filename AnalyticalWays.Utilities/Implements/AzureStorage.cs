using AnalyticalWays.Utilities.Interfaces;
using AnalyticalWays.Utilities.Model;
using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Text;
using System.Threading.Tasks;

namespace AnalyticalWays.Utilities.Implements {
    public class AzureStorage : IStorage {
        private readonly string _storageAccount = "analyticalwaysstorage";
        private readonly string _storageKey01 = "f9PRs7LgvguvpsV4nq1riT/aCbqLtMdz/N3fPs6ynr2kGpAkD2LSjMYhGAxoc6EZwDYvZHaWd43S5kfvK4lZ2w==";
        private readonly string _storageConnection01 = "DefaultEndpointsProtocol=https;AccountName=analyticalwaysstorage;AccountKey=f9PRs7LgvguvpsV4nq1riT/aCbqLtMdz/N3fPs6ynr2kGpAkD2LSjMYhGAxoc6EZwDYvZHaWd43S5kfvK4lZ2w==;EndpointSuffix=core.windows.net";
        private readonly string _storageKey02 = "auf93DPEQbNexTk0FmCw+W6WFA1dkjAgxavYY+zHuiWkqgG+9CcHHGQDMw2fJL5hYW1xWU4nALoKkjl93sdWSw==";
        private readonly string _storageConnection02 = "DefaultEndpointsProtocol=https;AccountName=analyticalwaysstorage;AccountKey=auf93DPEQbNexTk0FmCw+W6WFA1dkjAgxavYY+zHuiWkqgG+9CcHHGQDMw2fJL5hYW1xWU4nALoKkjl93sdWSw==;EndpointSuffix=core.windows.net";
        private readonly string _blobContainer = "csvfiles";


        public Task<bool> FileExists(string filename) {
            throw new NotImplementedException();
        }

        public Task<TMedatada> GetMetadata<TMedatada>(string filename) {
            throw new NotImplementedException();
        }

        public async Task<bool> ReadFile(string filename) {
            bool ejecutado = false;
            List<StockInformation> datos = new List<StockInformation>();
            try {
                Stopwatch timer = new Stopwatch();
                BlobServiceClient bsc = new BlobServiceClient(_storageConnection01);
                BlobContainerClient bcc = bsc.GetBlobContainerClient(_blobContainer);
                Response<bool> existsContainer = await bcc.ExistsAsync();
                if (existsContainer.Value) {
                    BlobClient bc = bcc.GetBlobClient(filename);
                    Response<bool> existsBlob = await bc.ExistsAsync();
                    if (existsBlob.Value) {
                        BlobDownloadInfo bdi = await bc.DownloadAsync();
                        if (File.Exists("descargado.csv")) {
                            File.Delete("descargado.csv");
                        }
                        BlobOpenReadOptions boro = new BlobOpenReadOptions(false);
                        // 100 MB => 6.216672551666667
                        boro.BufferSize = 100 * 1024 * 1024;
                        Console.WriteLine("Iniciando procesamiento de archivo");
                        timer.Start();
                        Stream stream = await bc.OpenReadAsync(boro);
                        int registrosBuffer = 0;
                        decimal registrosTotales = 0;
                        using StreamReader reader = new StreamReader(stream);
                        bool primeraLinea = true;
                        while (!reader.EndOfStream) {
                            string linea = await reader.ReadLineAsync();
                            if (primeraLinea) {
                                primeraLinea = false;
                                continue;
                            }
                            string[] componentes = linea.Split(";");
                            StockInformation info = new StockInformation() {
                                PointOfSale = componentes[0],
                                Product = componentes[1],
                                Date = componentes[2],
                                Stock = Convert.ToInt32(componentes[3])
                            };
                            datos.Add(info);
                            registrosBuffer += 1;
                            registrosTotales += 1;
                            if (registrosBuffer == 10000) {
                                Console.WriteLine($"Datos escritos: {registrosTotales}");
                                registrosBuffer = 0;
                            }
                        }
                        timer.Stop();
                        if (registrosBuffer != 0) {
                            Console.WriteLine($"Datos escritos al final: {registrosTotales}");
                        }
                        Console.WriteLine($"Finalizado procesamiento de archivo. Tiempo total (minutos): {timer.Elapsed.TotalMinutes}");
                    }
                }
            } catch (Exception ex) {
                _ = ex;
                throw;
            }
            return ejecutado;
        }
    }
}
