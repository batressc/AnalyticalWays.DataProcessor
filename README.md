# CSV Processor

* [Acerca de la aplicación](#acerca-de)
* [Instalación](#instalacion)
* [Paquetes utilizados](#paquetes)
* [Lógica de la aplicación](#logica-aplicacion)
* [Estructura de archivos de la aplicación](#archivos-aplicacion)
* [Aspectos relevantes](#aspectos-relevantes)



## <a name="acerca-de"></a>Acerca de la aplicación

**CSV Processor** es una aplicación que tiene como objetivo recuperar la información de un archivo CSV almacenado en un Blob Storage de
Microsoft Azure y guardarla en una base de datos SQL Server local.



## <a name="instalacion"></a>Instalación

Para poder ejecutar la aplicación deben realizarse los siguientes pasos:

### Herramientas de desarrollo y versión del framework

Para la codificación de la aplicación se han utilizado las siguientes versiones de herramientas y frameworks:

* Microsoft SQL Server 2014
* Visual Studio 2019 versión 16.6.7
* .NET Core 3.1
* C# 8.0

### Requisitos previos

1. Clonar o descargar el código fuente del repositorio [batressc/AnalyticalWays.DataProcessor](https://github.com/batressc/AnalyticalWays.DataProcessor)
2. Poseer una instancia local o remota de Microsoft SQL Server, **versión 2014 en adelante**
3. Opcional: Si se realizarán cambios en el modelo de datos, debe instalarse la extensión [EF Core Power Tools](https://marketplace.visualstudio.com/items?itemName=ErikEJ.EFCorePowerTools)

### Preparación de la base de datos

La preparación de la base de datos puede realizarse de diferentes formas. A continuación se indican algunos métodos:

##### Restauración de la copia de respaldo

En la carpeta raíz del proyecto, se encuentra el archivo de respaldo: [AnalyticalWaysTest.bak](https://github.com/batressc/AnalyticalWays.DataProcessor/blob/master/AnalyticalWaysTest.bak). Puede seguirse el proceso 
de restauración de base de datos tradicional mediante Management Studio. Para más detalles ver [el siguiente enlace](https://docs.microsoft.com/en-us/sql/relational-databases/backup-restore/quickstart-backup-restore-database?view=sql-server-ver15#restore-a-backup)

##### Ejecución de script

Si no se desea utilizar el archivo de respaldo o se tiene algún problema durante la restauración, puede ejecutarse el siguiente script desde el Management Studio, línea de comandos o el editor de su preferencia:

```sql
CREATE DATABASE [AnalyticalWaysTest]
GO

USE [AnalyticalWaysTest]
GO

CREATE TABLE [dbo].[StockInformation](
	[pos] [varchar](50) NOT NULL,
	[product] [varchar](50) NOT NULL,
	[stockDate] [date] NOT NULL,
	[stock] [int] NOT NULL,
 CONSTRAINT [PK_StockInformation] PRIMARY KEY CLUSTERED 
(
	[pos] ASC,
	[product] ASC,
	[stockDate] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO
```

Por defecto, el nombre de la base de datos generada es **"AnalyticalWaysTest".** Si se desea utilizar otro nombre para la base de datos es importante realizar la modificación correspondiente en la cadena de
conexión en el archivo [appsettings.json](https://github.com/batressc/AnalyticalWays.DataProcessor/blob/master/AnalyticalWays.DataProcessor/appsettings.json) de la aplicación.

### Cuenta de almacenamiento de Microsoft Azure

Se ha habilitado temporalmente una cuenta de almacenamiento en mi cuenta personal de Microsoft Azure donde se encuentra el archivo **Stock.CSV** en el contenedor de blobs **csvfiles**. Si se desea
modificar esta configuración deben modificarse los siguientes datos en el archivo [appsettings.json](https://github.com/batressc/AnalyticalWays.DataProcessor/blob/master/AnalyticalWays.DataProcessor/appsettings.json):

```json
"CsvProcessorConfiguration": {
	"BlobStorageConfiguration": {
			"ConnectionString": "<cadena de conexión de cuenta de almacenamiento>",
			"Container": "<nombre del contendor>",
			"FileName": "<nombre del archivo a leer (si aplica)>",
			...
		}
		...
}
```


 
## <a name="paquetes"></a>Paquetes utilizados

A continuación se listan los paquetes Nuget utilizados en el proyecto y una breve descripción de su función:

| Paquete | Uso |
| ------- | --- |
| [Azure.Storage.Blobs](https://github.com/Azure/azure-sdk-for-net/blob/Azure.Storage.Blobs_12.6.0/sdk/storage/Azure.Storage.Blobs/README.md) | Permite crear clientes especializados para la realización de operaciones de lectura, escritura y gestión de los contenedores de blobs en las cuentas de almacenamiento de Microsoft Azure. |
| [EFCore.BulkExtensions](https://github.com/borisdj/EFCore.BulkExtensions) | Proporciona diferentes métodos de extensión que agregan operaciones de [*bulk insert*](https://docs.microsoft.com/en-us/sql/t-sql/statements/bulk-insert-transact-sql?view=sql-server-ver15) en las entidades del DbContext de Entity Framework. |
| [Microsoft.EntityFrameworkCore.SqlServer](https://docs.microsoft.com/es-es/ef/core/) | Paquete de Entity Framework Core especializado para realizar conexiones de base de datos con SQL Server |
| [Microsoft.Extensions.Hosting](https://docs.microsoft.com/en-us/aspnet/core/fundamentals/host/generic-host?view=aspnetcore-3.1) | Proporciona métodos especializados para la creación de [*host genéricos*](https://docs.microsoft.com/en-us/dotnet/core/extensions/generic-host) en .NET Core |



## <a name="logica-aplicacion"></a>Lógica de la aplicación

Las tareas de descarga y procesamiento de archivos son muy frecuentes en las diversas aplicaciones de software, generalmente los pasos que involucran estas tareas 
son la lectura de los datos del archivo a procesar y una vez ha finalizado se ejecuta un proceso de transformación o almacenamiento. 

El problema de este esquema surge cuando se tienen archivos de gran tamaño ya que durante el proceso de lectura es común que no se pueda realizar ninguna acción, a esto debe 
sumársele las condiciones del entorno de almacenamiento, por ejemplo si el archivo está almacenado en la nube y la conexión a internet es de baja calidad o lenta.

**Debido a esto, el primer objetivo a buscar en la aplicación es implementar un sistema _"estilo streaming:_ Descargar una porción del archivo para que en paralelo pueda procesarse y
a la vez continuar con la descarga de la siguiente porción hasta su finalización.**

Si bien con la medida anterior se reduciría el tiempo muerto entre la descarga y el procesamiento, otro punto importante a tratar es el almacenamiento de la información en SQL Server.
Tradicionalmente almacenar una decena de registros no impacta en tiempo de ejecución de las aplicaciones, pero en este caso se trata de millones de registros. Para ello, SQL Server posee
operaciones especiales las cuales **permiten la inserción masiva de grandes cantidades de datos de forma eficiente: 
[_bulk insert_](https://docs.microsoft.com/en-us/sql/t-sql/statements/bulk-insert-transact-sql?view=sql-server-ver15). Esto sumado a la capacidad de ejecutar varias tareas en paralelo** 
aumentarian la eficiencia del procesamiento del bloque de datos.

Trabajar con múltiples procesos es una actividad que requiere cuidado ya que es necesario sincronizar entre los involucrados las operaciones de lectura y escritura para evitar interbloqueos
y duplicidad de lectura y/o procesamiento de la información. Para resolver este problema, se decidió la implementación del 
[patrón de productor/consumidor](https://www.iodocs.com/es/patrones-de-diseno-de-aplicaciones-productor-consumidor/) ya que se ajusta a las especificaciones del flujo de la aplicación:

* Se tiene uno o varios procesos encargados de la lectura del archivo CSV mediante _streaming_
* Se tienen uno o varios procesos encargados del procesamiento y envío de inforamción a SQL Server.



## <a name="archivos-aplicacion"></a>Estructura de archivos de la aplicación

El siguiente cuadro describe de forma general cada uno de los archivos de la aplicación

| Directorio | Archivo | Descripción |
| ---------- | ------- | ----------- |
| [Configuration](https://github.com/batressc/AnalyticalWays.DataProcessor/tree/master/AnalyticalWays.DataProcessor/Configuration) | N/A | Directorio donde se almacenan las clases utilizadas para el mapeo del las configuraciones de appsettings.json |
| [Configuration](https://github.com/batressc/AnalyticalWays.DataProcessor/tree/master/AnalyticalWays.DataProcessor/Configuration) | [BlobStorageConfiguration.cs](https://github.com/batressc/AnalyticalWays.DataProcessor/blob/master/AnalyticalWays.DataProcessor/Configuration/BlobStorageConfiguration.cs) | Parámetros de configuración relacionados con la cuenta de almacenamiento en Microsoft Azure |
| [Configuration](https://github.com/batressc/AnalyticalWays.DataProcessor/tree/master/AnalyticalWays.DataProcessor/Configuration) | [CsvProcessorConfiguration.cs](https://github.com/batressc/AnalyticalWays.DataProcessor/blob/master/AnalyticalWays.DataProcessor/Configuration/CsvProcessorConfiguration.cs) | Configuración general de aplicación |
| [Configuration](https://github.com/batressc/AnalyticalWays.DataProcessor/tree/master/AnalyticalWays.DataProcessor/Configuration) | [SQLProcessingConfiguration.cs](https://github.com/batressc/AnalyticalWays.DataProcessor/blob/master/AnalyticalWays.DataProcessor/Configuration/SQLProcessingConfiguration.cs) | Párametros de configuración relacionados con las operaciones de base de datos |
| [Configuration](https://github.com/batressc/AnalyticalWays.DataProcessor/tree/master/AnalyticalWays.DataProcessor/Configuration) | [TrackingConfiguration.cs](https://github.com/batressc/AnalyticalWays.DataProcessor/blob/master/AnalyticalWays.DataProcessor/Configuration/TrackingConfiguration.cs) | Parámetros de configuración relacionados con la generación del archivo de seguimiento de registros erróneos |
| [Contracts](https://github.com/batressc/AnalyticalWays.DataProcessor/tree/master/AnalyticalWays.DataProcessor/Contracts) | N/A | Directorio donde se almacenan las interfaces de las operaciones de lectura de archivos y procesamiento de datos en SQL  |
| [Contracts](https://github.com/batressc/AnalyticalWays.DataProcessor/tree/master/AnalyticalWays.DataProcessor/Contracts) | [IDataOperations.cs](https://github.com/batressc/AnalyticalWays.DataProcessor/blob/master/AnalyticalWays.DataProcessor/Contracts/IDataOperations.cs) | Interfaz con la definición de las operaciones a realizar sobre el repositorio de datos |
| [Contracts](https://github.com/batressc/AnalyticalWays.DataProcessor/tree/master/AnalyticalWays.DataProcessor/Contracts) | [IStorageOperations.cs](https://github.com/batressc/AnalyticalWays.DataProcessor/blob/master/AnalyticalWays.DataProcessor/Contracts/IStorageOperations.cs) | Interfaz con la definición de las operaciones a realizar sobre el espacio de almacenamiento de archivos |
| [Extensions](https://github.com/batressc/AnalyticalWays.DataProcessor/tree/master/AnalyticalWays.DataProcessor/Extensions) | N/A | Directorio donde se almacenan los métodos de extensión de la aplicación |
| [Extensions](https://github.com/batressc/AnalyticalWays.DataProcessor/tree/master/AnalyticalWays.DataProcessor/Extensions) | [IHostBuilderExtensions.cs](https://github.com/batressc/AnalyticalWays.DataProcessor/blob/master/AnalyticalWays.DataProcessor/Extensions/IHostBuilderExtensions.cs) | Posee métodos utilitarios para facilitar el mapeo de los servicios de la aplicación |
| [Implementations](https://github.com/batressc/AnalyticalWays.DataProcessor/tree/master/AnalyticalWays.DataProcessor/Implementations) | N/A | Implementaciones particulares de las operaciones de acceso a la cuenta de almacenamiento y acceso a SQL Server |
| [Implementations](https://github.com/batressc/AnalyticalWays.DataProcessor/tree/master/AnalyticalWays.DataProcessor/Implementations) | [ADODataOperations.cs](https://github.com/batressc/AnalyticalWays.DataProcessor/blob/master/AnalyticalWays.DataProcessor/Implementations/ADODataOperations.cs) | Implementación de operaciones de acceso a base de datos utilizando ADO.NET |
| [Implementations](https://github.com/batressc/AnalyticalWays.DataProcessor/tree/master/AnalyticalWays.DataProcessor/Implementations) | [AzureBlobStorageOperations.cs](https://github.com/batressc/AnalyticalWays.DataProcessor/blob/master/AnalyticalWays.DataProcessor/Implementations/AzureBlobStorageOperations.cs) | Implementación de operaciones de acceso a la cuenta de almacenamiento de Microsoft Azure |
| [Implementations](https://github.com/batressc/AnalyticalWays.DataProcessor/tree/master/AnalyticalWays.DataProcessor/Implementations) | [EntityFrameworkDataOperations.cs](https://github.com/batressc/AnalyticalWays.DataProcessor/blob/master/AnalyticalWays.DataProcessor/Implementations/EntityFrameworkDataOperations.cs) | Implementación de operaciones de acceso a base de datos utilizando Entity Framework y EF Bulk Extensions |
| [Model](https://github.com/batressc/AnalyticalWays.DataProcessor/tree/master/AnalyticalWays.DataProcessor/Model) | N/A  | Directorio donde se almacena el modelo de datos de la aplicación |
| [Model](https://github.com/batressc/AnalyticalWays.DataProcessor/tree/master/AnalyticalWays.DataProcessor/Model) | [AnalyticalWaysTestDbContext.cs](https://github.com/batressc/AnalyticalWays.DataProcessor/blob/master/AnalyticalWays.DataProcessor/Model/AnalyticalWaysTestDbContext.cs) | DbContext de la aplicación autogenerado utilizando la extensión de EF Core Power Tools |
| [Model](https://github.com/batressc/AnalyticalWays.DataProcessor/tree/master/AnalyticalWays.DataProcessor/Model) | [StockInformation.cs](https://github.com/batressc/AnalyticalWays.DataProcessor/blob/master/AnalyticalWays.DataProcessor/Model/StockInformation.cs) | Entidad autogenerada utilizando la extensión de EF Core Power Tools |
| N/A | [Consumer.cs](https://github.com/batressc/AnalyticalWays.DataProcessor/blob/master/AnalyticalWays.DataProcessor/Consumer.cs) | Consumidor: Lee la inforamción pre-procesada por el proceso productor y escribe los datos en SQL Server |
| N/A | [CsvProcessor.cs](https://github.com/batressc/AnalyticalWays.DataProcessor/blob/master/AnalyticalWays.DataProcessor/CsvProcessor.cs) | **Clase principal encargada del ejecutar el proceso de lectura y almacenamiento de datos en SQL Server** |
| N/A | [Producer.cs](https://github.com/batressc/AnalyticalWays.DataProcessor/blob/master/AnalyticalWays.DataProcessor/Producer.cs) | Productor: Obtiene la información del archivo CSV ubicado en la cuenta de almacenamiento de Microsoft Azure y lo prepara para ser procesado por los procesos consumidores |
| N/A | [Program.cs](https://github.com/batressc/AnalyticalWays.DataProcessor/blob/master/AnalyticalWays.DataProcessor/Program.cs) | Punto de inicio de la aplicación como host |
| N/A | [Startup.cs](https://github.com/batressc/AnalyticalWays.DataProcessor/blob/master/AnalyticalWays.DataProcessor/Startup.cs) | Clase que almacena la configuración de las inyecciones de dependencias y servicios de la aplicación |
| N/A | [appsettings.json](https://github.com/batressc/AnalyticalWays.DataProcessor/blob/master/AnalyticalWays.DataProcessor/appsettings.json) | Archivo de configuración de la aplicación |
| N/A | [efpt.config.json](https://github.com/batressc/AnalyticalWays.DataProcessor/blob/master/AnalyticalWays.DataProcessor/efpt.config.json) | Archivo autogenerado que posee la configuración de conexión utilizando la extensión de EF Core Power Tools |



## <a name="aspectos-relevantes"></a>Aspectos relevantes

### Detalles de implementación

La aplicación se ha codificado como un proyecto monolítico con el objetivo de simplificar la solución, sin embargo esto no evita que puedan separarse cada uno de los componentes de aplicación en proyectos separados según su función para formar parte de un proyecto más grande y complejo.

Se ha implementado un [Worker Service](https://docs.microsoft.com/en-us/aspnet/core/fundamentals/host/hosted-services?view=aspnetcore-3.1&tabs=visual-studio) alojado en una aplicación de consola con .NET Core 3.1 con el objetivo de aprovechar las características de configuración e inyección de dependencias del tipo de proyecto.

Se han creado interfaces tanto para las operaciones de lectura del archivo CSV como del acceso de datos a SQL con el objetivo de poder probar diversas implementaciones de estas operaciones y facilitar la investigación y determinación de las tecnologías a utilizar para la implementación más eficiente.

### Channels: El componente central de la aplicación

Como se comentó en la sección de la [lógica de aplicación](#logica-aplicacion) la optimización del tiempo de descarga y procesamiento se logra gracias la implementación del [patrón de productor/consumidor](https://www.iodocs.com/es/patrones-de-diseno-de-aplicaciones-productor-consumidor/). Debido a que es un problema común en el
desarrollo de software, Microsoft nos proporciona el espacio de nombres [System.Threading.Channels](https://docs.microsoft.com/en-us/dotnet/api/system.threading.channels?view=netcore-3.1) el cual provee de forma nativa diversas clases que permiten la sincronización de datos enviando datos de foma asíncrona entre productores
y consumidores. En el siguiente [enlace](https://devblogs.microsoft.com/dotnet/an-introduction-to-system-threading-channels/) se puede obtener información detallada de su uso e implementación.

En la aplicación la implementación del Channel se ha realizado de la siguiente forma:

##### Proceso principal
En el siguiente fragmento de código del archivo [CsvProcessor.cs](https://github.com/batressc/AnalyticalWays.DataProcessor/blob/master/AnalyticalWays.DataProcessor/CsvProcessor.cs), el cual es el proceso principal de la aplicación, se crea la instancia del Channel utilizado para la sincronización de lectura del archivo CSV 
(productor) y escritura de datos en SQL Server (consumidores). 

```csharp
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
```

Dado que la información que se va a compartir son objetos del tipo [StockInformation](https://github.com/batressc/AnalyticalWays.DataProcessor/blob/master/AnalyticalWays.DataProcessor/Model/StockInformation.cs) la inicialización del objeto Channel se realiza de la 
siguiente forma:

```csharp
// Creando Channel para gestión nativa de patrón productor-consumidor
Channel<StockInformation> canal = Channel.CreateUnbounded<StockInformation>();
```

Posteriormente se crean las tareas correspondientes para el productor (lector de archivo CSV) y el consumidor (escritores a SQL Server). Es imporante mencionar que al utilizar el método `Channel.CreateUnbounded<StockInformation>()` estamos especificando que nuestro
Channel puede tener cualquier cantidad de productores y consumidores. Para la aplicación, el esquema que se ha seguido es de **un solo productor, múltiples consumidores**.

Definición de tarea productora:

```csharp
// Creando procesos
List<Task<TimeSpan>> procesos = new List<Task<TimeSpan>> {
    Task.Run(() => _consumer.PrepararDatos(_conf.BlobStorageConfiguration.FileName, canal, ctoken, cts))
};
```

`Definición de tareas consumidoras:

```csharp
for (int i = 1; i <= tareas; i++) {
    int proceso = i;
    procesos.Add(Task.Run(() => _producer.GuardarDatos(proceso, canal, ctoken, cts)));
}
```

##### Productor

La clase [Producer.cs](https://github.com/batressc/AnalyticalWays.DataProcessor/blob/master/AnalyticalWays.DataProcessor/Producer.cs) posee él método `PrepararDatos` el cual realiza las tareas del productor, éste recibe como parámetro la instancia del objeto `Channel<StockInformation>` utilizado para la sincronización
de datos.

```csharp
public async Task<TimeSpan> PrepararDatos(string filename, Channel<StockInformation> channel, CancellationToken cancellationToken, CancellationTokenSource cts)
```

En el siguiente fragmento de código del mismo método podemos observar que utilizando el método `Writer.WriteAsync` agregamos la información que queremos esté dipsonible para nuestros consumidores. Una vez ya no recibimos más información, mediante el método `Writer.Complete` indicamos al Channel que no se recibirá más
información.

```csharp
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
```

##### Consumidor

La clase [Consumer.cs](https://github.com/batressc/AnalyticalWays.DataProcessor/blob/master/AnalyticalWays.DataProcessor/Consumer.cs) posee él método `GuardarDatos` el cual realiza las tareas del consumidor, éste recibe como parámetro la instancia del objeto `Channel<StockInformation>` utilizado para la sincronización
de datos.

```csharp
public async Task<TimeSpan> GuardarDatos(int proceso, Channel<StockInformation> channel, CancellationToken cancellationToken, CancellationTokenSource cts)
```

En el siguiente fragmento de código del mismo método podemos observar que utlizando el método `Reader.WaitToReadAsync` las tareas consumidoras determinan si el Channel posee datos disponibles para consumo y mediante el método `Reader.ReadAsync` es posible recuperar la información almacenada en el Channel.

```csharp
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
```

### Prueba de almacenamiento de datos utilizando Entity Framework

Por defecto, la aplicación utiliza la implementación de operaciones de base de datos **con ADO.NET** debido a que <u>es la tecnología más eficiente para el almacenamiento de datos.</u> Como parte de las actividades de investigación realizadas también se implementó una versión de operaciones utilizando **Entity Framework Core** pero 
adicionándole los métodos de extensión de _bulk insert_ del paquete [EFCore.BulkExtensions](https://github.com/borisdj/EFCore.BulkExtensions). Esta combinación de librerías se convirtió **la segunda implementación más eficiente** para almacenar la información.

Para utilizar esta implementación solamente debe cambiarse el código del método **ConfigureServices** del archivo [Startup.cs](https://github.com/batressc/AnalyticalWays.DataProcessor/blob/master/AnalyticalWays.DataProcessor/Startup.cs) de la siguiente forma:

```csharp
public void ConfigureServices(IServiceCollection services) {
    // Mapeo de configuración a clase
    services.Configure<CsvProcessorConfiguration>(Configuration.GetSection("CsvProcessorConfiguration"));
    // Configuración de servicios
    // NOTA: Quitar comentarios entre las líneas 22 a 29 y comentar línea 30 si se desea probar mediante EF Core
    services.AddDbContext<AnalyticalWaysTestDbContext>(options => {
        options.UseSqlServer(Configuration.GetConnectionString("AnalyticalWaysDatabase"), opt => {
            opt.EnableRetryOnFailure();
        });
        options.EnableDetailedErrors(true);
        options.EnableSensitiveDataLogging(true);
    });
    services.AddTransient<IDataOperations<StockInformation>, EntityFrameworkDataOperations>();
    //services.AddTransient<IDataOperations<StockInformation>, ADODataOperations>();
    services.AddTransient<IStorageOperations, AzureBlobStorageOperations>();
    services.AddTransient<Producer>();
    services.AddTransient<Consumer>();
    // Proceso de tratamiento de archivo CSV
    services.AddHostedService<CsvProcessor>();
}
```

### Visualización de ejecución de aplicación

Se utiliza la implementación estandar del servicio `ILogger<T>` para la visualización de los diversos mensajes de seguimiento en la aplicación. Dependiendo del nivel de seguimiento 
configurado en el archivo [appsettings.json](https://github.com/batressc/AnalyticalWays.DataProcessor/blob/master/AnalyticalWays.DataProcessor/appsettings.json) así serán visibles los 
mensajes en la consola de la aplicación.

Se recomienda utilizar las siguientes configuraciones:

##### Visualización resumida

Mediante esta configuración solamente serán visibles los mensajes iniciales de inicio de la aplicación y los tiempos finales del procesamiento. Para ello, en el archivo 
[appsettings.json](https://github.com/batressc/AnalyticalWays.DataProcessor/blob/master/AnalyticalWays.DataProcessor/appsettings.json) debe configurarse la sección **Logging** de la 
siguiente forma:

```json
"Console": {
    "LogLevel": {
        "Default": "Information"
    }
}
```

En la siguiente imagen se puede visualizar la forma de presentación de mensajes en la consola de la aplicación una vez ha finalizado su ejecución.

![Consola de aplicación en modo resumen](http://batressc.com/analyticalways/EjecucionInformacion.PNG)

##### Visualización detallada

Mediante esta configuración se muestran todos los mensajes seguimiento del proceso de carga de información a SQL Server. Para ello, en el archivo 
[appsettings.json](https://github.com/batressc/AnalyticalWays.DataProcessor/blob/master/AnalyticalWays.DataProcessor/appsettings.json) debe configurarse la sección **Logging** de la 
siguiente forma:

```json
"Console": {
    "LogLevel": {
        "Default": "Debug"
    }
}
```

En la siguiente imagen se puede visualizar la forma de presentación de mensajes en la consola de la aplicación al iniciar el procesamiento.

![Consola de aplicación en modo debug - inicio de aplicación](http://batressc.com/analyticalways/EjecucionDebug00.PNG)

En la siguiente imagen se puede visualizar los registros de seguimiento del tiempo de procesamiento por cada tarea.

![Consola de aplicación en modo debug - inicio de aplicación](http://batressc.com/analyticalways/EjecucionDebug01.PNG)


















