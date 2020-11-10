using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using System;
using System.Reflection;

namespace Microsoft.Extensions.Hosting {
    /// <summary>
    /// Extensiones utilitarias para IHostBuider
    /// </summary>
    public static class IHostBuilderExtensions {
        private const string _configureServiceMethod = "ConfigureServices";
        /// <summary>
        /// Permite la configuración de los servicios utilizando una clase de inicialización del tipo <typeparamref name="TStartup"/>
        /// </summary>
        /// <typeparam name="TStartup">Tipo de la clase de inicialización</typeparam>
        /// <param name="hostBuilder">Instancia de IHostBuilder</param>
        /// <returns>IHostBuilder con servicios configurados</returns>
        public static IHostBuilder UseStartup<TStartup>(this IHostBuilder hostBuilder) {
            // Determinando si la clase posee el método "ConfigureServices" donde se realiza la configuración de servicios
            MethodInfo configureServiceMethod = typeof(TStartup).GetMethod(_configureServiceMethod, new Type[] { typeof(IServiceCollection) });
            if (configureServiceMethod != null) {
                // Buscando si la clase posee constructor con IConfiguration
                ConstructorInfo startupConstructor = typeof(TStartup).GetConstructor(new Type[] { typeof(IConfiguration) });
                // Realizando la configuración de los servicios
                hostBuilder.ConfigureServices((ctx, services) => {
                    // Creando instancia de clase Startup
                    TStartup startup;
                    if (startupConstructor != null) {
                        startup = (TStartup)Activator.CreateInstance(typeof(TStartup), ctx.Configuration);
                    } else {
                        startup = Activator.CreateInstance<TStartup>();
                    }
                    // Ejecutando método de configuración
                    configureServiceMethod.Invoke(startup, new object[] { services });
                });
            }
            return hostBuilder;
        }
    }
}
