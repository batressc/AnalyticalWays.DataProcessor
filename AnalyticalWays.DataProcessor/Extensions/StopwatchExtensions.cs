using System;
using System.Threading.Tasks;

namespace System.Diagnostics {
    public static class StopwatchExtensions {
        public static TimeSpan Cronometrar(this Stopwatch cronometro, Action accion) {
            cronometro.Start();
            accion.Invoke();
            cronometro.Stop();
            TimeSpan tiempoResultante = cronometro.Elapsed;
            cronometro.Reset();
            return tiempoResultante;
        }
    }
}
