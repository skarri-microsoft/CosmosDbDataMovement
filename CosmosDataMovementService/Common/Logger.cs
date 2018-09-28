namespace Common
{
    using System;
    using System.Diagnostics;

    public class Logger
    {
        public static void LogError(string errorMessage, bool throwError)
        {
            Trace.TraceError(errorMessage);
            if (throwError)
            {
                throw new Exception(errorMessage);
            }
        }
    }
}
