using ASConsultaEstadoEnvio;
using Limilabs.Client.POP3;
using Limilabs.Mail;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using OpenPop.Mime;
using OpenPop.Pop3;
using RestSharp;
using System;
using System.Collections.Generic;
using System.Drawing;
using System.Drawing.Imaging;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Mail;
using System.Net.NetworkInformation;
using System.Reflection;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Timers;
using System.Web.Script.Serialization;
using System.Xml;
using System.Xml.Linq;



namespace TareasProgramadasAgroDTE
{
    class Program
    {
        public static string Consola_log = "";
        public static string servidor_boletas = "api";//Produccion: api o rahue, Certificacion: apicert o pangal
        public static string servidor_facturas = "palena";//Produccion: palena, Certificacion: maullin
        public static string servidor_PortalSII = "zeusr.sii.cl";//Produccion: zeusr.sii.cl, Certificacion: 
        public static string RutEmisor = ""; //Rut de la empresa registrada en el SII
        public static string RutEnvia = ""; //Rut del emisor de documentos registrado en el SII
        public static string FchResol = ""; // Fecha de la resolucion
        public static string NroResol = ""; //Numero de la resolucion
        public static string MailSistema = ""; //Correo asignado al sistema para que envie correos automaticos
        public static string PassSistema = ""; //Contraseña del correo del sistema
        public static string MailCobranza = ""; //Correo del area de cobranza
        public static string directorio_archivos = ""; //Direcotrio de archivos
        public static string datetime_str = DateTime.Now.ToString("yyyy/MM/dd HH:mm:ss");
        public static bool es_produccion = true; //Produccion: true
        private static Mutex mutex = new Mutex();

        static void Main(string[] args)
        {
            CargarDatos();


            //EJECUTAR CADA 15 MINUTOS //VERIFICADO--------------------------------------------------------------
            System.Timers.Timer eTimer = new System.Timers.Timer();
            eTimer.Elapsed += new ElapsedEventHandler(ExecuteChequearEstadoEnvioBoletas);
            eTimer.Interval = 900000; //INTERVALO DE TIEMPO DE EJECUCION DEL PROGRAMA
            eTimer.Enabled = true;


            //EJECUTAR CADA 15 MINUTOS // VERIFICADO -----------------------------------------------------------
            System.Timers.Timer aTimer = new System.Timers.Timer();
            aTimer.Elapsed += new ElapsedEventHandler(ExecuteChequearEstadoEnvioFacturas);
            aTimer.Interval = 900000; //INTERVALO DE TIEMPO DE EJECUCION DEL PROGRAMA
            aTimer.Enabled = true;


            //EJECUTAR CADA 30 MINUTOS VERIFICADO------------------------------------------------------------
            System.Timers.Timer bTimer = new System.Timers.Timer();
            bTimer.Elapsed += new ElapsedEventHandler(ExecuteEnviarSobresCliente);
            bTimer.Interval = 1800000; //INTERVALO DE TIEMPO DE EJECUCION DEL PROGRAMA
            bTimer.Enabled = true;

            //EJECUTAR CADA 30 MIN. // VERIFICADO--------------------------------------------------------------
            System.Timers.Timer cTimer = new System.Timers.Timer();
            cTimer.Elapsed += new ElapsedEventHandler(ExecuteVerificarVigenciaTokenBoletas);
            cTimer.Interval = 1800000; //INTERVALO DE TIEMPO DE EJECUCION DEL PROGRAMA
            cTimer.Enabled = true;

            //EJECUTAR CADA 25 MIN. // VERIFICADO--------------------------------------------------------------
            System.Timers.Timer iTimer = new System.Timers.Timer();
            iTimer.Elapsed += new ElapsedEventHandler(ExecuteVerificarVigenciaTokenPortalSII);
            iTimer.Interval = 1500000; //INTERVALO DE TIEMPO DE EJECUCION DEL PROGRAMA
            iTimer.Enabled = true;

            //EJECUTAR CADA 6 HORAS //VERIFICADOOO   ---------------------------------------------------------
            System.Timers.Timer dTimer = new System.Timers.Timer();
            dTimer.Elapsed += new ElapsedEventHandler(ExecuteLimpiarConsola);
            dTimer.Interval = 21600000; //INTERVALO DE TIEMPO DE EJECUCION DEL PROGRAMA
            dTimer.Enabled = true;

            //EJECUTAR CADA 8 HORAS //VERIFICADOOO   ---------------------------------------------------------
            //System.Timers.Timer fTimer = new System.Timers.Timer();
            //fTimer.Elapsed += new ElapsedEventHandler(ExecuteEnviarConsumoFolios);
            //fTimer.Interval = 28800000; //INTERVALO DE TIEMPO DE EJECUCION DEL PROGRAMA
            //fTimer.Enabled = true;


            //EJECUTAR CADA 1 HORAS //VERIFICADO --------------------------------------------------------------
            System.Timers.Timer gTimer = new System.Timers.Timer();
            gTimer.Elapsed += new ElapsedEventHandler(ExecuteDescargarFacturaCompra);
            gTimer.Interval = 1800000; //INTERVALO DE TIEMPO DE EJECUCION DEL PROGRAMA
            gTimer.Enabled = true;

            //EJECUTAR CADA 2 HORAS // VERIFICADO ------------------------------------------------------------
            System.Timers.Timer hTimer = new System.Timers.Timer();
            hTimer.Elapsed += new ElapsedEventHandler(ExecuteActualizarEstadoRespuesta);
            hTimer.Interval = 3600000; //INTERVALO DE TIEMPO DE EJECUCION DEL PROGRAMA
            hTimer.Enabled = true;

            //EJECUTAR CADA 5 HORAS // VERIFICADO ------------------------------------------------------------
            System.Timers.Timer oTimer = new System.Timers.Timer();
            oTimer.Elapsed += new ElapsedEventHandler(ExecuteVerificarCAF);
            oTimer.Interval = 9000000; //INTERVALO DE TIEMPO DE EJECUCION DEL PROGRAMA
            oTimer.Enabled = true;

            //EJECUTAR CADA 5 HORAS // VERIFICADO ------------------------------------------------------------
            System.Timers.Timer vTimer = new System.Timers.Timer();
            vTimer.Elapsed += new ElapsedEventHandler(ExecuteVerificarAcusesClientes);
            vTimer.Interval = 9000000; //INTERVALO DE TIEMPO DE EJECUCION DEL PROGRAMA
            vTimer.Enabled = true;

            //EJECUTAR CADA 5 min  // VERIFICADO ------------------------------------------------------------
            System.Timers.Timer v3Timer = new System.Timers.Timer();
            v3Timer.Elapsed += new ElapsedEventHandler(ExecuteEnviarSobreSII);
            v3Timer.Interval = 300000; //INTERVALO DE TIEMPO DE EJECUCION DEL PROGRAMA
            v3Timer.Enabled = true;

            Console.WriteLine("Si desea ejecutar un metodo manualmente, Favor escribir el numero metodo:");



            for (int i = 0; i < 1000; i++)
            {
                string input = Console.ReadLine();

                switch (input)
                {
                    case "1":
                        Console.WriteLine("Ejecuto manualmente: ChequearEstadoEnvioBoletas()");
                        System.Timers.Timer zTimer = new System.Timers.Timer(2000);
                        zTimer.Elapsed += new ElapsedEventHandler(ExecuteChequearEstadoEnvioBoletas);
                        zTimer.AutoReset = false;
                        zTimer.Start();


                        break;
                    case "2":
                        Console.WriteLine("Ejecuto manualmente: ChequearEstadoEnvioFacturas()");
                        System.Timers.Timer xTimer = new System.Timers.Timer(2000);
                        xTimer.Elapsed += new ElapsedEventHandler(ExecuteChequearEstadoEnvioFacturas);
                        xTimer.AutoReset = false;
                        xTimer.Start();
                        break;
                    case "3":
                        Console.WriteLine("Ejecuto manualmente: EnviarSobresCliente()");
                        System.Timers.Timer yTimer = new System.Timers.Timer(2000);
                        yTimer.Elapsed += new ElapsedEventHandler(ExecuteEnviarSobresCliente);
                        yTimer.AutoReset = false;
                        yTimer.Start();
                        break;
                    case "4":
                        Console.WriteLine("Ejecuto manualmente: VerificarVigenciaTokenBoletas()");
                        System.Timers.Timer kTimer = new System.Timers.Timer(2000);
                        kTimer.Elapsed += new ElapsedEventHandler(ExecuteVerificarVigenciaTokenBoletas);
                        kTimer.AutoReset = false;
                        kTimer.Start();
                        break;
                    case "5":
                        Console.WriteLine("Ejecuto manualmente: LimpiarConsola()");
                        System.Timers.Timer jTimer = new System.Timers.Timer(2000);
                        jTimer.Elapsed += new ElapsedEventHandler(ExecuteLimpiarConsola);
                        jTimer.AutoReset = false;
                        jTimer.Start();
                        break;
                    case "6":
                        Console.WriteLine(" EnviarConsumoFolios() esta descontinuada");
                        /*System.Timers.Timer vTimer = new System.Timers.Timer(2000);
                        vTimer.Elapsed += new ElapsedEventHandler(ExecuteEnviarConsumoFolios);
                        vTimer.AutoReset = false;
                        vTimer.Start();*/
                        break;
                    case "7":
                        Console.WriteLine("Ejecuto manualmente: DescargarFacturaCompra()");
                        System.Timers.Timer nTimer = new System.Timers.Timer(2000);
                        nTimer.Elapsed += new ElapsedEventHandler(ExecuteDescargarFacturaCompra);
                        nTimer.AutoReset = false;
                        nTimer.Start();
                        break;
                    case "8":
                        Console.WriteLine("Ejecuto manualmente: ActualizarEstadoRespuesta()");
                        System.Timers.Timer mTimer = new System.Timers.Timer(2000);
                        mTimer.Elapsed += new ElapsedEventHandler(ExecuteActualizarEstadoRespuesta);
                        mTimer.AutoReset = false;
                        mTimer.Start();
                        break;
                    case "9":
                        Console.WriteLine("Ejecuto manualmente: VerificarVigenciaTokenPortalSII()");
                        System.Timers.Timer wTimer = new System.Timers.Timer(2000);
                        wTimer.Elapsed += new ElapsedEventHandler(ExecuteVerificarVigenciaTokenPortalSII);
                        wTimer.AutoReset = false;
                        wTimer.Start();
                        break;
                    case "10":
                        Console.WriteLine("Ejecuto manualmente: VerificarCAF()");
                        System.Timers.Timer pTimer = new System.Timers.Timer(2000);
                        pTimer.Elapsed += new ElapsedEventHandler(ExecuteVerificarCAF);
                        pTimer.AutoReset = false;
                        pTimer.Start();
                        break;
                    case "11":
                        Console.WriteLine("Ejecuto manualmente: VerificarAcusesClientes()");
                        System.Timers.Timer v2Timer = new System.Timers.Timer(2000);
                        v2Timer.Elapsed += new ElapsedEventHandler(ExecuteVerificarAcusesClientes);
                        v2Timer.AutoReset = false;
                        v2Timer.Start();
                        break;
                    case "12":
                        Console.WriteLine("Ejecuto manualmente: EnviarSobreSII()");
                        System.Timers.Timer v4Timer = new System.Timers.Timer(2000);
                        v4Timer.Elapsed += new ElapsedEventHandler(ExecuteEnviarSobreSII);
                        v4Timer.AutoReset = false;
                        v4Timer.Start();
                        break;
                    default:
                        Console.WriteLine("No se reconocio el numero digitado, o metodo no existe");
                        break;
                }

                //Console.WriteLine("Se escribio: " + input);

            }





            Console.ReadLine();
        }

        public static string getFecha()
        {
            return datetime_str = DateTime.Now.ToString("yyyy/MM/dd HH:mm:ss");
        }



        public static void CargarDatos()
        {
            ConexionBD conexion = new ConexionBD();
            List<string> datos_empresa = conexion.SelectDatosempresa("SELECT rut_empresa,rut_rrll,DATE_FORMAT(fecha_res, '%Y-%m-%d') AS fecha ,numero_res,mail_sistema_empresa,pass_sistema_empresa,mail_cobranza_empresa FROM empresa WHERE id_empresa=1");

            RutEmisor = datos_empresa[0];
            RutEnvia = datos_empresa[1];
            FchResol = datos_empresa[2];
            NroResol = datos_empresa[3];
            MailSistema = datos_empresa[4];
            PassSistema = datos_empresa[5];
            MailCobranza = datos_empresa[6];

            //VERIFICAR SI EXISTE EL DIRECTORIO EN C:\AgroDTE_Archivos\
            directorio_archivos = @"C:\inetpub\wwwroot\api_agrodte\AgroDTE_Archivos\";
            verificarDirectorio(directorio_archivos);

            string build_version = Assembly.GetExecutingAssembly().GetName().Version.ToString();
            string mensaje = "Bienvenido a <AgroDTE> Tareas Automaticas" + Environment.NewLine;
            mensaje = mensaje + "Version: " + build_version + Environment.NewLine;
            mensaje = mensaje + "Fecha inicializacion: " + getFecha() + Environment.NewLine;
            mensaje = mensaje + "-----------------------------------------" + Environment.NewLine;
            mensaje = mensaje + "1. ChequearEstadoEnvioBoletas() : SE EJECUTA CADA 15MIN" + Environment.NewLine;
            mensaje = mensaje + "2. ChequearEstadoEnvioFacturas() : SE EJECUTA CADA 15MIN" + Environment.NewLine;
            mensaje = mensaje + "3. EnviarSobresCliente() : SE EJECUTA CADA 30MIN" + Environment.NewLine;
            mensaje = mensaje + "4. VerificarVigenciaTokenBoletas() : SE EJECUTA CADA 30MIN" + Environment.NewLine;
            mensaje = mensaje + "5. LimpiarConsola() : SE EJECUTA CADA 6 HORA" + Environment.NewLine;
            mensaje = mensaje + "6. EnviarConsumoFolios() : ***DESCONTINUADA***" + Environment.NewLine;
            mensaje = mensaje + "7. DescargarFacturaCompra() : SE EJECUTA CADA 1 HORA" + Environment.NewLine;
            mensaje = mensaje + "8. ActualizarEstadoRespuesta() : SE EJECUTA CADA 2 HORAS" + Environment.NewLine;
            mensaje = mensaje + "9. VerificarVigenciaTokenPortalSII() : SE EJECUTA CADA 25 MINUTOS" + Environment.NewLine;
            mensaje = mensaje + "10. VerificarCAF() : SE EJECUTA CADA 5 HORAS" + Environment.NewLine;
            mensaje = mensaje + "11. VerificarAcusesClientes() : SE EJECUTA CADA 5 HORAS" + Environment.NewLine;
            mensaje = mensaje + "12. EnviarSobreSII() : SE EJECUTA CADA 5 MINUTOS" + Environment.NewLine;

            Consola_log = Consola_log + mensaje + Environment.NewLine;


            Console.WriteLine(mensaje);



        }

        public static void ExecuteVerificarAcusesClientes(object source, ElapsedEventArgs e)
        {
            mutex.WaitOne();
            string mensaje = "[" + getFecha() + "]: Ejecutando VerificarAcusesClientes()";
            Consola_log = Consola_log + mensaje + Environment.NewLine;
            Console.WriteLine(mensaje);

            VerificarAcusesClientes();

            mensaje = "[" + getFecha() + "]: Finalizando VerificarAcusesClientes()";
            Consola_log = Consola_log + mensaje + Environment.NewLine;
            Console.WriteLine(mensaje);
            mutex.ReleaseMutex();
        }
        public static void ExecuteEnviarSobreSII(object source, ElapsedEventArgs e)
        {
            mutex.WaitOne();
            string mensaje = "[" + getFecha() + "]: Ejecutando EnviarSobreSII()";
            Consola_log = Consola_log + mensaje + Environment.NewLine;
            Console.WriteLine(mensaje);

            EnviarSobreSII();

            mensaje = "[" + getFecha() + "]: Finalizando EnviarSobreSII()";
            Consola_log = Consola_log + mensaje + Environment.NewLine;
            Console.WriteLine(mensaje);
            mutex.ReleaseMutex();
        }
        public static void ExecuteEnviarConsumoFolios(object source, ElapsedEventArgs e)
        {
            mutex.WaitOne();
            string mensaje = "[" + getFecha() + "]: Ejecutando ExecuteEnviarConsumoFolios()";
            Consola_log = Consola_log + mensaje + Environment.NewLine;
            Console.WriteLine(mensaje);


            //RESCATAMOS LA FECHA DEL DIA ANTERIOR
            DateTime dateForButton = DateTime.Now.AddDays(-1);
            string fecha_anterior = dateForButton.ToString("yyyy-MM-dd");
            //PREGUNTAR SI HAY REGISTRO DEL DIA ANTERIOR

            ConexionBD conexion = new ConexionBD();
            List<string> resultado_consumo_folios = conexion.Select("SELECT id_consumo_folios FROM consumo_folios WHERE fecha_inicio = '" + fecha_anterior + "'");


            if (resultado_consumo_folios.Count != 0)
            {
                //EXISTE REGISTRO DE UN CONSUMO DE FOLIOS
                Console.WriteLine("[" + getFecha() + "]: EXISTE REGISTRO DE CONSUMO DE FOLIOS, ID: " + resultado_consumo_folios[0]);
                Consola_log = Consola_log + "[" + getFecha() + "]: EXISTE REGISTRO DE CONSUMO DE FOLIOS, ID: " + resultado_consumo_folios[0] + Environment.NewLine;
            }
            else
            {
                //NO HAY REGISTRO DE UN CONSUMO DE FOLIOS
                Console.WriteLine("[" + getFecha() + "]: NO EXISTE REGISTRO, PREPARANDO ARCHIVO CONSUMO DE FOLIOS...");
                Consola_log = Consola_log + "[" + getFecha() + "]: NO EXISTE REGISTRO, PREPARANDO ARCHIVO CONSUMO DE FOLIOS..." + Environment.NewLine;
                EnviarConsumoFolios();
            }

            mensaje = "[" + getFecha() + "]: Finalizando ExecuteEnviarConsumoFolios()";
            Consola_log = Consola_log + mensaje + Environment.NewLine;
            Console.WriteLine(mensaje);

            mutex.ReleaseMutex();


        }
        public static void ExecuteDescargarFacturaCompra(object source, ElapsedEventArgs e)
        {
            mutex.WaitOne();
            string mensaje = "[" + getFecha() + "]: Ejecutando descargarFacturaCompra()";
            Consola_log = Consola_log + mensaje + Environment.NewLine;
            Console.WriteLine(mensaje);

            descargarFacturaCompra();

            mensaje = "[" + getFecha() + "]: Finalizando descargarFacturaCompra()";
            Consola_log = Consola_log + mensaje + Environment.NewLine;
            Console.WriteLine(mensaje);
            mutex.ReleaseMutex();
        }
        public static void ExecuteLimpiarConsola(object source, ElapsedEventArgs e)
        {
            mutex.WaitOne();
            string mensaje = "[" + getFecha() + "]: Ejecutando ExecuteLimpiarConsola()";
            Consola_log = Consola_log + mensaje + Environment.NewLine;
            Console.WriteLine(mensaje);


            string fecha_actual_str = DateTime.Now.ToString("yyyyMMddHHmmss");
            //VERIFICAR SI EXISTE EL DIRECTORIO EN C:\AgroDTE_Archivos\
            directorio_archivos = @"C:\inetpub\wwwroot\api_agrodte\AgroDTE_Archivos\";
            verificarDirectorio(directorio_archivos);

            try
            {
                Directory.CreateDirectory(@"C:\inetpub\wwwroot\api_agrodte\AgroDTE_Archivos\Log\");
                File.WriteAllText(@"C:\inetpub\wwwroot\api_agrodte\AgroDTE_Archivos\Log\ConsoleLog" + fecha_actual_str + ".txt", Consola_log);
                ConexionBD conexion = new ConexionBD();
                conexion.Consulta("INSERT INTO log_event (mensaje_log_event, fecha_log_event, referencia_log_event, query_request_log_event) VALUES ('Registro de consola Tareas Programadas', NOW(), 'Tareas Programadas', '" + Consola_log + "');");
                Consola_log = "";


            }
            catch (Exception ex)
            {
                mensaje = "[" + getFecha() + "]: HUBO UN PROBLEMA AL CREAR DIRECTORIO: " + ex.Message;
                Console.WriteLine(mensaje);
                Consola_log = Consola_log + mensaje + Environment.NewLine;
            }

            Console.Clear();

            mensaje = "[" + getFecha() + "]: Finalizado ExecuteLimpiarConsola()";
            Consola_log = Consola_log + mensaje + Environment.NewLine;
            Console.WriteLine(mensaje);

            mensaje = Environment.NewLine;
            mensaje = mensaje + "-----------------------------------------" + Environment.NewLine;
            mensaje = mensaje + "1. ChequearEstadoEnvioBoletas() : SE EJECUTA CADA 15MIN" + Environment.NewLine;
            mensaje = mensaje + "2. ChequearEstadoEnvioFacturas() : SE EJECUTA CADA 15MIN" + Environment.NewLine;
            mensaje = mensaje + "3. EnviarSobresCliente() : SE EJECUTA CADA 30MIN" + Environment.NewLine;
            mensaje = mensaje + "4. VerificarVigenciaTokenBoletas() : SE EJECUTA CADA 30MIN" + Environment.NewLine;
            mensaje = mensaje + "5. LimpiarConsola() : SE EJECUTA CADA 6 HORAS" + Environment.NewLine;
            mensaje = mensaje + "6. EnviarConsumoFolios() : ****DESCONTINUADA****" + Environment.NewLine;
            mensaje = mensaje + "7. DescargarFacturaCompra() : SE EJECUTA CADA 1 HORA" + Environment.NewLine;
            mensaje = mensaje + "8. ActualizarEstadoRespuesta() : SE EJECUTA CADA 2 HORAS" + Environment.NewLine;
            mensaje = mensaje + "9. VerificarVigenciaTokenPortalSII() : SE EJECUTA CADA 25 MINUTOS" + Environment.NewLine;
            mensaje = mensaje + "10. VerificarCAF() : SE EJECUTA CADA 5 HORAS" + Environment.NewLine;
            mensaje = mensaje + "11. VerificarAcusesClientes() : SE EJECUTA CADA 5 HORAS" + Environment.NewLine;
            mensaje = mensaje + "12. EnviarSobreSII() : SE EJECUTA CADA 5 MINUTOS" + Environment.NewLine;
            mensaje = mensaje + Environment.NewLine;
            mensaje = mensaje + "Si desea ejecutar un metodo manualmente, Favor escribir el numero metodo:";

            Console.WriteLine(mensaje);

            Consola_log = Consola_log + mensaje + Environment.NewLine;

            mutex.ReleaseMutex();

            // Starts a new instance of the program itself
            System.Diagnostics.Process.Start(System.AppDomain.CurrentDomain.FriendlyName);

            // Closes the current process
            Environment.Exit(0);
        }
        private static void ExecuteChequearEstadoEnvioFacturas(object source, ElapsedEventArgs e)
        {
            mutex.WaitOne();
            string mensaje = "[" + getFecha() + "]: Ejecutando validarSobresEnvio() de Facturas";
            Consola_log = Consola_log + mensaje + Environment.NewLine;
            Console.WriteLine(mensaje);

            //Chequear que hora es
            TimeSpan horaActual = DateTime.Now.TimeOfDay;
            validarSobresEnvio(horaActual, servidor_facturas);

            mensaje = "[" + getFecha() + "]: Finalizando validarSobresEnvio() de Facturas";
            Consola_log = Consola_log + mensaje + Environment.NewLine;
            Console.WriteLine(mensaje);
            mutex.ReleaseMutex();
        }
        private static void ExecuteChequearEstadoEnvioBoletas(object source, ElapsedEventArgs e)
        {
            mutex.WaitOne();
            //Chequear que hora es
            string mensaje = "[" + getFecha() + "]: Ejecutando validarSobresEnvio() de Boletas";
            Consola_log = Consola_log + mensaje + Environment.NewLine;
            Console.WriteLine(mensaje);

            TimeSpan horaActual = DateTime.Now.TimeOfDay;
            validarSobresEnvio(horaActual, servidor_boletas);

            mensaje = "[" + getFecha() + "]: Finalizando validarSobresEnvio() de Boletas";
            Consola_log = Consola_log + mensaje + Environment.NewLine;
            Console.WriteLine(mensaje);
            mutex.ReleaseMutex();
        }
        private static void ExecuteEnviarSobresCliente(object source, ElapsedEventArgs e)
        {
            mutex.WaitOne();
            string mensaje = "[" + getFecha() + "]: Ejecutando EnviarSobresClientes()";
            Consola_log = Consola_log + mensaje + Environment.NewLine;
            Console.WriteLine(mensaje);

            EnviarSobresClientes();

            mensaje = "[" + getFecha() + "]: Finalizando EnviarSobresClientes()";
            Consola_log = Consola_log + mensaje + Environment.NewLine;
            Console.WriteLine(mensaje);
            mutex.ReleaseMutex();
        }
        public static void ExecuteVerificarVigenciaTokenBoletas(object source, ElapsedEventArgs e)
        {
            /*VERIFICAR LA VALIDEZ DE UN TOKEN, EL TOKEN DURA 1 HORA APROX.
           VERFICAR CAMPO DE fecha_ultimo_uso_token SE HA USADO EN LOS ULTIMOS 30MIN
           SERVIDORES: https://apicert.sii.cl/recursos/v1 - Certificacion Boletas Token
                       https://api.sii.cl/recursos/v1 - Produccion Boletas Token
                       https://maullin.sii.cl/DTEWS/GetTokenFromSeed.jws - Certificacion facturas token
                      https://palena.sii.cl/DTEWS/CrSeed.jws?WSDL  - Producion Facturas Token
           */
            mutex.WaitOne();
            string mensaje = "[" + getFecha() + "]: Ejecutando VerificarVigenciaToken()";
            Consola_log = Consola_log + mensaje + Environment.NewLine;
            Console.WriteLine(mensaje);

            VerificarVigenciaToken(servidor_boletas);

            mensaje = "[" + getFecha() + "]: Finalizando VerificarVigenciaToken()";
            Consola_log = Consola_log + mensaje + Environment.NewLine;
            Console.WriteLine(mensaje);
            mutex.ReleaseMutex();
        }

        public static void ExecuteVerificarVigenciaTokenPortalSII(object source, ElapsedEventArgs e)
        {
            /*VERIFICAR LA VALIDEZ DE UN TOKEN, EL TOKEN DURA 1 HORA APROX.
           VERFICAR CAMPO DE fecha_ultimo_uso_token SE HA USADO EN LOS ULTIMOS 30MIN
           SERVIDORES: https://apicert.sii.cl/recursos/v1 - Certificacion Boletas Token
                       https://api.sii.cl/recursos/v1 - Produccion Boletas Token
                       https://maullin.sii.cl/DTEWS/GetTokenFromSeed.jws - Certificacion facturas token
                      https://palena.sii.cl/DTEWS/CrSeed.jws?WSDL  - Producion Facturas Token
           */
            mutex.WaitOne();
            string mensaje = "[" + getFecha() + "]: Ejecutando VerificarVigenciaTokenPortalSII()";
            Consola_log = Consola_log + mensaje + Environment.NewLine;
            Console.WriteLine(mensaje);

            VerificarVigenciaToken(servidor_PortalSII);

            mensaje = "[" + getFecha() + "]: Finalizando VerificarVigenciaTokenPortalSII()";
            Consola_log = Consola_log + mensaje + Environment.NewLine;
            Console.WriteLine(mensaje);
            mutex.ReleaseMutex();
        }
        public static void ExecuteVerificarVigenciaTokenFacturas(object source, ElapsedEventArgs e)
        {
            /*VERIFICAR LA VALIDEZ DE UN TOKEN, EL TOKEN DURA 1 HORA APROX.
           VERFICAR CAMPO DE fecha_ultimo_uso_token SE HA USADO EN LOS ULTIMOS 30MIN
           SERVIDORES: https://apicert.sii.cl/recursos/v1 - Certificacion Boletas Token
                       https://api.sii.cl/recursos/v1 - Produccion Boletas Token
                       https://maullin.sii.cl/DTEWS/GetTokenFromSeed.jws - Certificacion facturas token
                      https://palena.sii.cl/DTEWS/CrSeed.jws?WSDL  - Producion Facturas Token
           */


          
            VerificarVigenciaToken(servidor_facturas);
           
        }
        public static void ExecuteActualizarEstadoRespuesta(object source, ElapsedEventArgs e)
        {
            mutex.WaitOne();
            string mensaje = "[" + getFecha() + "]: Ejecutando actualizarEstadosRespuesta()";
            Consola_log = Consola_log + mensaje + Environment.NewLine;
            Console.WriteLine(mensaje);

            actualizarEstadosRespuesta();

            mensaje = "[" + getFecha() + "]: Finalizando actualizarEstadosRespuesta()";
            Consola_log = Consola_log + mensaje + Environment.NewLine;
            Console.WriteLine(mensaje);
            mutex.ReleaseMutex();

        }

        public static void ExecuteVerificarCAF(object source, ElapsedEventArgs e)
        {
            mutex.WaitOne();
            string mensaje = "[" + getFecha() + "]: Ejecutando VerificarCAF()";
            Consola_log = Consola_log + mensaje + Environment.NewLine;
            Console.WriteLine(mensaje);

            verificarVigenciaCaf();

            mensaje = "[" + getFecha() + "]: Finalizando actualizarVerificarCAF()";
            Consola_log = Consola_log + mensaje + Environment.NewLine;
            Console.WriteLine(mensaje);
            mutex.ReleaseMutex();

        }

        public static void VerificarAcusesClientes()
        {
            ConexionBD conexion = new ConexionBD();
            List<string> list_datos_finales = new List<string>();
            try
            {
                for (int i = 0; i < 4; i++)
                {
                    string tabla = "";
                    switch (i)
                    {
                        case 0:
                            tabla = "factura";
                            break;
                        case 1:
                            tabla = "nota_credito";
                            break;
                        case 2:
                            tabla = "nota_debito";
                            break;
                        case 3:
                            tabla = "guia_despacho";
                            break;
                        default:
                            break;
                    }
                    for (int j = 0; j < 2; j++) //CICLO DE LOS 3 TIPOS DE ACUSE: ENVIO,MERCADERIA Y COMERCIAL
                    {
                        string tipo_acuse = "";
                        switch (j)
                        {
                            case 0: //ACUSE ENVIO
                                tipo_acuse = "id_acuse_recibo_cliente_fk";
                                break;
                            case 1: //ACUSE COMERCIAL
                                tipo_acuse = "id_acuse_recibo_comercial_cliente_fk";
                                break;
                            case 2: //ACUSE MERCADERIAS
                                tipo_acuse = "id_acuse_recibo_mercaderia_cliente_fk";
                                break;
                            default:
                                break;
                        }
                        string query_id_acuse = "SELECT JSON_OBJECT('folio',folio_" + tabla + ",'id_acuse', " + tipo_acuse + ",'fecha', fchemis_" + tabla + ", 'razon_social', rznsocrecep_" + tabla + ") FROM " + tabla + " WHERE " + tipo_acuse + " IS NOT NULL AND fchemis_" + tabla + " BETWEEN date_sub(NOW(),INTERVAL 1 WEEK) and NOW()";
                        List<string> list_folio_id = conexion.Select_Json(query_id_acuse); //LISTA QUE GUARDA LOS FOLIOS DEL DTE Y LOS ID DEL ACUSE DE LA ULTIMA SEMANA


                        //PREGUNTAR ESE CODIGO EN LA TABLA DE ACUSES CORRESPONDIENTE
                        for (int o = 0; o < list_folio_id.Count; o++)
                        {
                            var json_data = JsonConvert.DeserializeObject<JObject>(list_folio_id[o]);
                            string id_acuse = json_data["id_acuse"].ToString();
                            string razon_social = json_data["razon_social"].ToString();
                            string folio = json_data["folio"].ToString();
                            string fecha = json_data["fecha"].ToString();
                            //CON EL ID DEL ACUSE VAMOS PREGUNTAR A LA TABLA
                            string query_estado_acuse = "";

                            switch (tipo_acuse)
                            {
                                case "id_acuse_recibo_cliente_fk": //ACUSE ENVIO
                                    query_estado_acuse = "SELECT acuse_recibo_cliente.estado_recep_env_acuse_recibo_cliente,acuse_recibo_cliente.recep_env_glosa_acuse_recibo_cliente,correo_intercambio.casilla_correo FROM acuse_recibo_cliente INNER JOIN correo_intercambio ON acuse_recibo_cliente.uid_correo = correo_intercambio.uid_correo WHERE acuse_recibo_cliente.id_acuse_recibo_cliente = '" + id_acuse + "'";
                                    break;
                                case "id_acuse_recibo_comercial_cliente_fk": //ACUSE COMERCIAL
                                    query_estado_acuse = "SELECT acuse_recibo_comercial_dte_cliente.estado_dte_acuse_recibo_comercial_dte_cliente,acuse_recibo_comercial_dte_cliente.glosa_dte_acuse_recibo_comercial_dte_cliente,correo_intercambio.casilla_correo FROM acuse_recibo_comercial_dte_cliente INNER JOIN acuse_recibo_comercial_cliente ON acuse_recibo_comercial_cliente.id_acuse_recibo_comercial_cliente = acuse_recibo_comercial_dte_cliente.id_acuse_recibo_comercial_cliente_fk INNER JOIN  correo_intercambio ON acuse_recibo_comercial_cliente.uid_correo = correo_intercambio.uid_correo WHERE acuse_recibo_comercial_dte_cliente.id_acuse_recibo_comercial_cliente_fk = '" + id_acuse + "'";
                                    break;
                                case "id_acuse_recibo_mercaderia_cliente_fk": //ACUSE MERCADERIA
                                    query_estado_acuse = "SELECT acuso_recibo_dte_mercaderia_cliente.rutfirma_acuse_recibo_dte_mercaderia_cliente,acuso_recibo_dte_mercaderia_cliente.declaracion_acuse_recibo_dte_mercaderia_cliente, correo_intercambio.casilla_correo FROM acuso_recibo_dte_mercaderia_cliente INNER JOIN acuse_recibo_mercaderia_cliente ON acuse_recibo_mercaderia_cliente.id_acuse_recibo_mercaderia_cliente = acuso_recibo_dte_mercaderia_cliente.id_acuse_recibo_mercaderia_cliente_fk INNER JOIN correo_intercambio ON  acuse_recibo_mercaderia_cliente.uid_correo = correo_intercambio.uid_correo WHERE id_acuse_recibo_mercaderia_cliente_fk = '" + id_acuse + "'";
                                    break;
                                default:
                                    break;
                            }

                            List<string> list_estado = conexion.Select(query_estado_acuse);


                            if (tipo_acuse == "id_acuse_recibo_cliente_fk" || tipo_acuse == "id_acuse_recibo_comercial_cliente_fk")
                            {
                                if (list_estado[0] == "0" && tipo_acuse == "id_acuse_recibo_cliente_fk")
                                {
                                    //BUSCAR EN LA TABLA DETALLE DEL ACUSE DE ENVIO PARA SABER SI TUVO UN RECHAZO

                                    string query_estado_acuse_dte_envio = "SELECT estado_recep_acuse_recibo_cliente,glosa_estado_acuse_recibo_cliente FROM acuso_recibo_dte_cliente WHERE id_acuse_recibo_cliente_fk = " + id_acuse + "";
                                    List<string> list_estado_dte_envio = conexion.Select(query_estado_acuse_dte_envio);

                                    if ((list_estado_dte_envio != null))
                                    {
                                        if (list_estado_dte_envio[0] != "0")
                                        {
                                            //SE GRABA GLOSA
                                            list_datos_finales.Add(folio); //AGREGAMOS FOLIO
                                            list_datos_finales.Add(razon_social); //AGREGAMOS RAZON SOCIAL
                                            list_datos_finales.Add(fecha); // AGREGAMOS LA FECHA DE EMISION DEL DTE
                                            list_datos_finales.Add(list_estado_dte_envio[1]); //AGREGAMOS LA GLOSA
                                            list_datos_finales.Add(list_estado[2]); //AGREGAMOS EL CORREO DE INTERCAMBIO DTE

                                        }

                                    }
                                }
                                if (list_estado[0] != "0" && tipo_acuse == "id_acuse_recibo_comercial_cliente_fk")
                                {
                                    //SE GRABA GLOSA
                                    list_datos_finales.Add(folio);//AGREGAMOS FOLIO
                                    list_datos_finales.Add(razon_social);//AGREGAMOS RAZON SOCIAL
                                    list_datos_finales.Add(fecha);// AGREGAMOS LA FECHA DE EMISION DEL DTE
                                    list_datos_finales.Add(list_estado[1]);//AGREGAMOS LA GLOSA
                                    list_datos_finales.Add(list_estado[2]);//AGREGAMOS EL CORREO DE INTERCAMBIO DTE


                                }

                            }
                            if (tipo_acuse == "id_acuse_recibo_mercaderia_cliente_fk")
                            {
                                if (!list_estado[1].Contains("servicio(s) prestado(s) ha(n) sido recibido(s)"))
                                {
                                    //GRABAR GLOSA
                                    list_datos_finales.Add(folio);//AGREGAMOS FOLIO
                                    list_datos_finales.Add(razon_social);//AGREGAMOS RAZON SOCIAL
                                    list_datos_finales.Add(fecha);// AGREGAMOS LA FECHA DE EMISION DEL DTE
                                    list_datos_finales.Add(list_estado[1]);//AGREGAMOS LA GLOSA
                                    list_datos_finales.Add(list_estado[2]);//AGREGAMOS EL CORREO DE INTERCAMBIO DTE

                                }

                            }


                        }





                    }


                }

                if (list_datos_finales.Count() != 0)
                {
                    var date = DateTime.Now; 

                    if (date.Hour > 9 && date.Hour < 17) //ENVIAR CORREO EN HORARIO LABORAL
                    {
                        //MANDAR CORREO
                        string mensajeCorreo = "<html><body>"
                        + "<h3> AVISO DE RECHAZOS EN DTE:</h3>"
                        + "<div>"
                        + "<p> Hemos detectado rechazos por parte de cliente en los siguientes documentos:</p>"
                        + "<ul>";

                        //RESCATAMOS LA IMAGEN
                        Bitmap bitmap = new Bitmap(@"C:\inetpub\wwwroot\api_agrodte\AgroDTE_Archivos\logo_AgroDTE_OFICIAL.png");
                        System.IO.MemoryStream ms = new MemoryStream();
                        bitmap.Save(ms, ImageFormat.Png);
                        byte[] byteImage = ms.ToArray();
                        var SigBase64 = Convert.ToBase64String(byteImage);

                        for (int i = 0; i < list_datos_finales.Count; i = i + 5)
                        {
                            mensajeCorreo = mensajeCorreo + "<li  style=\"margin: 10px 0;\"><b> Folio:</b> " + list_datos_finales[i] + " | <b> Razon Social:</b> " + list_datos_finales[i + 1] + " | <b> Fecha Emision:</b> " + list_datos_finales[i + 2] + " | <b> Glosa o Motivo:</b> " + list_datos_finales[i + 3] + " | <b> Correo intercambio DTE:</b> " + list_datos_finales[i + 4] + " </li>";
                        }
                        mensajeCorreo = mensajeCorreo + "  </ul>"
                                                    + "<p style = \"font-size: 13px;\"> Correo generado automatico por AgroDTE.</p>"
                                                         + "</div>";
                        mensajeCorreo = mensajeCorreo + @"<img src=""data:image/png;base64," + SigBase64 + @"""   width=""220"" height=""100"">";
                        mensajeCorreo = mensajeCorreo + "</body>"
                                                       + "</html>";
                        string asunto = "AVISO DE RECHAZOS DTE";

                        try
                        {

                            SmtpClient mySmtpClient = new SmtpClient("mail.agroplastic.cl");

                            // set smtp-client with basicAuthentication
                            mySmtpClient.UseDefaultCredentials = false;
                            System.Net.NetworkCredential basicAuthenticationInfo = new
                               System.Net.NetworkCredential(MailSistema, PassSistema);
                            mySmtpClient.Credentials = basicAuthenticationInfo;

                            // add from,to mailaddresses
                            System.Net.Mail.MailAddress from = new System.Net.Mail.MailAddress(MailSistema, "AgroDTE");
                            //MailAddress to = new MailAddress("bmcortes@agroplastic.cl, mriquelme@agroplastic.cl", "Benjamin");
                            MailMessage myMail = new MailMessage();
                            myMail.To.Add(MailCobranza); // MailCobranza
                            myMail.To.Add("mriquelme@agroplastic.cl");
                            //myMail.To.Add("agrodte@agroplastic.cl");
                            myMail.From = from;



                            // add ReplyTo
                            //MailAddress replyTo = new MailAddress("mriquelme@agroplastic.cl");
                            //myMail.ReplyToList.Add(replyTo);

                            // set subject and encoding
                            myMail.Subject = asunto;
                            myMail.SubjectEncoding = System.Text.Encoding.UTF8;

                            // set body-message and encoding
                            myMail.Body = mensajeCorreo;
                            myMail.BodyEncoding = System.Text.Encoding.UTF8;
                            // text or html
                            myMail.IsBodyHtml = true;

                            mySmtpClient.Send(myMail);
                        }

                        catch (SmtpException ex)
                        {
                            throw new ApplicationException
                              ("SmtpException has occured: " + ex.Message);
                        }

                    }

                }
            }
            catch (Exception e)
            {
                Console.WriteLine("HUBO UN ERROR EN VerificarAcusesClientes() :" + e.Message);
                throw;
            }


        }


        public static void actualizarEstadosRespuesta()
        {
            string mensaje = "[" + getFecha() + "]: ACTUALIZANDO ESTADO ACUSE DE RESPUESTA DTE DE COMPRA...";
            Console.WriteLine(mensaje);
            Consola_log = Consola_log + mensaje + Environment.NewLine;


            //ACTUAÑIZA, LOS ESTADOS DE CADA DTE SI NO SON RESPONDIDOS MANUALMENTE A LOS 8 DIAS
            try
            {
                ConexionBD conexion = new ConexionBD();

                string strFechaActual = DateTime.Now.Date.ToString("yyyy/MM/dd");
                string query_idFecha_facturaCompra = "SELECT  id_factura_compra,fchemis_factura_compra FROM factura_compra WHERE estado_acuse_factura_compra = '0'";
                string query_idFecha_facturaExentaCompra = "SELECT id_factura_exenta_compra,fchemis_factura_exenta_compra FROM factura_exenta_compra WHERE estado_acuse_factura_exenta_compra = '0'";
                string query_idFecha_guiaDespachoCompra = "SELECT id_guia_despacho_compra,fchemis_guia_despacho_compra FROM guia_despacho_compra WHERE estado_acuse_guia_despacho_compra = '0'";
                string query_idFecha_notaCreditoCompra = "SELECT id_nota_credito_compra,fchemis_nota_credito_compra FROM nota_credito_compra WHERE estado_acuse_nota_credito_compra = '0'";
                string query_idFecha_notaDebitoCompra = "SELECT id_nota_debito_compra,fchemis_nota_debito_compra FROM nota_debito_compra WHERE estado_acuse_nota_debito_compra = '0'";
                //string query_diferenciaFechas = "SELECT TIMESTAMPDIFF(DAY, (SELECT fchemis_factura_compra FROM factura_compra WHERE estado_acuse_factura_compra = '0'), (SELECT CURDATE())) AS dias_transcurridos;";
                //SELECT TIMESTAMPDIFF(DAY, (SELECT fchemis_factura_compra FROM factura_compra WHERE id_factura_compra = '1'), (SELECT CURDATE())) AS dias_transcurridosfactura_compra

                List<string> listIdFecha_facturaCompra = conexion.Select(query_idFecha_facturaCompra);
                List<string> listIdFecha_facturaExentaCompra = conexion.Select(query_idFecha_facturaExentaCompra);
                List<string> listIdFecha_guiaDespachoCompra = conexion.Select(query_idFecha_guiaDespachoCompra);
                List<string> listIdFecha_notaCreditoCompra = conexion.Select(query_idFecha_notaCreditoCompra);
                List<string> listIdFecha_notaDebitoCompra = conexion.Select(query_idFecha_notaDebitoCompra);

                List<string> listFechasEmision_facturaCompra = new List<string>();
                List<string> listId_facturaCompra = new List<string>();
                List<string> listFechasEmision_facturaExentaCompra = new List<string>();
                List<string> listId_facturaExentaCompra = new List<string>();
                List<string> listFechasEmision_guiaDespachoCompra = new List<string>();
                List<string> listId_guiaDespachoCompra = new List<string>();
                List<string> listFechasEmision_notaCreditoCompra = new List<string>();
                List<string> listId_notaCreditoCompra = new List<string>();
                List<string> listFechasEmision_notaDebitoCompra = new List<string>();
                List<string> listId_notaDebitoCompra = new List<string>();

                for (int i = 0; i < listIdFecha_facturaCompra.Count; i += 2)
                {
                    listId_facturaCompra.Add(listIdFecha_facturaCompra[i]);
                    listFechasEmision_facturaCompra.Add(listIdFecha_facturaCompra[i + 1]);
                }

                for (int i = 0; i < listIdFecha_facturaExentaCompra.Count; i += 2)
                {
                    listId_facturaExentaCompra.Add(listIdFecha_facturaExentaCompra[i]);
                    listFechasEmision_facturaExentaCompra.Add(listIdFecha_facturaExentaCompra[i + 1]);
                }

                for (int i = 0; i < listIdFecha_guiaDespachoCompra.Count; i += 2)
                {
                    listId_guiaDespachoCompra.Add(listIdFecha_guiaDespachoCompra[i]);
                    listFechasEmision_guiaDespachoCompra.Add(listIdFecha_guiaDespachoCompra[i + 1]);
                }

                for (int i = 0; i < listIdFecha_notaCreditoCompra.Count; i += 2)
                {
                    listId_notaCreditoCompra.Add(listIdFecha_notaCreditoCompra[i]);
                    listFechasEmision_notaCreditoCompra.Add(listIdFecha_notaCreditoCompra[i + 1]);
                }

                for (int i = 0; i < listIdFecha_notaDebitoCompra.Count; i += 2)
                {
                    listId_notaDebitoCompra.Add(listIdFecha_notaDebitoCompra[i]);
                    listFechasEmision_notaDebitoCompra.Add(listIdFecha_notaDebitoCompra[i + 1]);
                }

                for (int i = 0; i < listFechasEmision_facturaCompra.Count; i++)
                {
                    DateTime dateFechaEmision = DateTime.Parse(listFechasEmision_facturaCompra[i]);
                    DateTime dateFechaActual = DateTime.Parse(strFechaActual);

                    TimeSpan timeDias = dateFechaActual - dateFechaEmision;

                    int intDias = timeDias.Days;

                    if (intDias >= 8)
                    {
                        string strUpdateEstado = "UPDATE factura_compra SET estado_acuse_factura_compra = '3' WHERE id_factura_compra = '" + listId_facturaCompra[i] + "'";
                        conexion.Consulta(strUpdateEstado);
                    }

                }

                for (int i = 0; i < listFechasEmision_facturaExentaCompra.Count; i++)
                {
                    DateTime dateFechaEmision = DateTime.Parse(listFechasEmision_facturaExentaCompra[i]);
                    DateTime dateFechaActual = DateTime.Parse(strFechaActual);

                    TimeSpan timeDias = dateFechaActual - dateFechaEmision;

                    int intDias = timeDias.Days;

                    if (intDias >= 8)
                    {
                        string strUpdateEstado = "UPDATE factura_exenta_compra SET estado_acuse_factura_exenta_compra = '3' WHERE id_factura_exenta_compra = '" + listId_facturaExentaCompra[i] + "'";
                        conexion.Consulta(strUpdateEstado);
                    }

                }

                for (int i = 0; i < listFechasEmision_guiaDespachoCompra.Count; i++)
                {
                    DateTime dateFechaEmision = DateTime.Parse(listFechasEmision_guiaDespachoCompra[i]);
                    DateTime dateFechaActual = DateTime.Parse(strFechaActual);

                    TimeSpan timeDias = dateFechaActual - dateFechaEmision;

                    int intDias = timeDias.Days;

                    if (intDias >= 8)
                    {
                        string strUpdateEstado = "UPDATE guia_despacho_compra SET estado_acuse_guia_despacho_compra = '3' WHERE id_guia_despacho_compra = '" + listId_guiaDespachoCompra[i] + "'";
                        conexion.Consulta(strUpdateEstado);
                    }

                }

                for (int i = 0; i < listFechasEmision_notaCreditoCompra.Count; i++)
                {
                    DateTime dateFechaEmision = DateTime.Parse(listFechasEmision_notaCreditoCompra[i]);
                    DateTime dateFechaActual = DateTime.Parse(strFechaActual);

                    TimeSpan timeDias = dateFechaActual - dateFechaEmision;

                    int intDias = timeDias.Days;

                    if (intDias >= 8)
                    {
                        string strUpdateEstado = "UPDATE nota_credito_compra SET estado_acuse_nota_credito_compra = '3' WHERE id_nota_credito_compra = '" + listId_notaCreditoCompra[i] + "'";
                        conexion.Consulta(strUpdateEstado);
                    }

                }

                for (int i = 0; i < listFechasEmision_notaDebitoCompra.Count; i++)
                {
                    DateTime dateFechaEmision = DateTime.Parse(listFechasEmision_notaDebitoCompra[i]);
                    DateTime dateFechaActual = DateTime.Parse(strFechaActual);

                    TimeSpan timeDias = dateFechaActual - dateFechaEmision;

                    int intDias = timeDias.Days;

                    if (intDias >= 8)
                    {
                        string strUpdateEstado = "UPDATE nota_debito_compra SET estado_acuse_nota_debito_compra = '3' WHERE id_nota_debito_compra = '" + listId_notaDebitoCompra[i] + "'";
                        conexion.Consulta(strUpdateEstado);
                    }

                }

                mensaje = "[" + getFecha() + "]: FINALIZADO ACTUALIZAR ESTADOS DE ACUSE DE RESPUESTA DTE DE COMPRA";
                Console.WriteLine(mensaje);
                Consola_log = Consola_log + mensaje + Environment.NewLine;
            }
            catch (Exception ex)
            {
                mensaje = "[" + getFecha() + "]: HUBO UN ERROR EN ActualizarEstadosRespuesta() : "+ex.Message;
                Console.WriteLine(mensaje);
                Consola_log = Consola_log + mensaje + Environment.NewLine;


            }

           

        }
        public static string EnvioSobreFactura(string rutaXml)
        {
            rutaXml = rutaXml.Replace("\\", "\\\\");
            string path = "http://192.168.1.9:90/WebServiceEnvioDTE/EnvioSobreDTE.asmx/enviarSobreSII?archivo=" + rutaXml + "&rutEmisor=6402678-k&rutEmpresa=76958430-7";
            WebRequest request = WebRequest.Create(path);
            request.Method = "GET";
            WebResponse response = request.GetResponse();
            string respuestaEnvio = "";
            using (var reader = new StreamReader(response.GetResponseStream()))
            {
                respuestaEnvio = reader.ReadToEnd(); // do something fun...
            }

            return respuestaEnvio;

        }
        public static void EnviarSobreSII()
        {
            string rutEmpresa = RutEmisor;
            string rutEmisor = RutEnvia;


            //TRAER TODOS LOS ENVIO CON ESTADO No Enviado EN UN INTERVALO DE LA ULTIMA SEMANA
            ConexionBD conexion = new ConexionBD();
            string queryNoEnviados = "SELECT id_envio_dte FROM envio_dte WHERE estado_envio_dte = \"No Enviado\" && fecha_envio_dte BETWEEN date_sub(NOW(),INTERVAL 1 WEEK) and NOW()";
            List<string> listNoEnviados = conexion.Select(queryNoEnviados);

            if (listNoEnviados.Count() == 0)
            {
                string mensaje = "NO HAY DOCUMENTOS PARA ENVIAR EN FUNCION EnviarSobreSII() ";
                Console.WriteLine(mensaje);
                Consola_log = Consola_log + mensaje + Environment.NewLine;
            }
            else
            {
                //HACER UN CICLO POR CADA DTE QUE HAY QUE ENVIAR
                for (int i = 0; i < listNoEnviados.Count(); i++)
                {

                    string queryRutaXml = "SELECT rutaxml_envio_dte FROM envio_dte WHERE id_envio_dte = '" + listNoEnviados[i] + "'";
                    List<string> listRutaXml = conexion.Select(queryRutaXml);

                    if (listRutaXml.Count() == 0)
                    {
                        string mensaje = "NO EXISTE EL SOBRE DEL DTE EN FUNCION EnviarSobreSII() TRACKID: "+ listNoEnviados[i];
                        Console.WriteLine(mensaje);
                        Consola_log = Consola_log + mensaje + Environment.NewLine;
                    }
                    else
                    {
                        //CARGAR EL XML PARA SABER QUE TIPO DE DTE ES
                        XmlDocument MyDoc = new XmlDocument();
                        try
                        {
                            MyDoc.Load(listRutaXml[0]);
                            string etiqueta = MyDoc.LastChild.Name;
                            if (etiqueta == "EnvioDTE")
                            {
                                //ES FACTURA
                                string rutaXml = listRutaXml[0];
                                string respuestaEnvio = EnvioSobreFactura(rutaXml);
                                string TrackId_str = "";
                                XmlDocument xmlDoc2 = new XmlDocument();
                                xmlDoc2.LoadXml(respuestaEnvio);
                                XmlNodeList TrackId = xmlDoc2.GetElementsByTagName("string");
                                TrackId_str = TrackId[0].InnerXml;

                                //SI EL SII ENVIA UN TRACK ID 0 EJECUTAMOS OTRA VEZ EL ENVIAR SOBRE

                                if (TrackId_str == "0" || TrackId_str == "HEFESTO.DTE.AUTENTICACION.ENT.Respuesta")
                                {
                                    for (int j = 0; i < 3; j++)
                                    {
                                        respuestaEnvio = EnvioSobreFactura(rutaXml);
                                        XmlDocument xmlDoc3 = new XmlDocument();
                                        xmlDoc2.LoadXml(respuestaEnvio);
                                        XmlNodeList TrackId2 = xmlDoc2.GetElementsByTagName("string");
                                        TrackId_str = TrackId2[0].InnerXml;
                                        if (TrackId_str != "0" || TrackId_str != "HEFESTO.DTE.AUTENTICACION.ENT.Respuesta")
                                        {
                                            break;
                                        }
                                    }

                                }

                               

                                if (ulong.TryParse(TrackId_str, out ulong numeroEnvio))
                                {
                                    //UPDATEAR EL NUMERO DE TRACKID, ESTADO,  EN LA FACTURA 
                                    if (TrackId_str == "0")
                                    {
                                        string mensaje = "HUBO UN PROBLEMA CON EL NUMERO TRACKID DE FACTURA EN FUNCION EnviarSobreSII() ID_ENVIO: " + listNoEnviados[i] + "TRACK ID: " + TrackId_str;
                                        Console.WriteLine(mensaje);
                                        Consola_log = Consola_log + mensaje + Environment.NewLine;
                                    }
                                    else
                                    {
                                        string queryUpdateEstado = "UPDATE envio_dte SET  estado_envio_dte = 'Enviado', trackid_envio_dte = '" + numeroEnvio + "' WHERE id_envio_dte = '" + listNoEnviados[i] + "';";
                                        conexion.Consulta(queryUpdateEstado);
                                        string mensaje = "DTE ENVIADO TRACKID : " + TrackId_str + " ID ENVIO: " + listNoEnviados[i];
                                        Console.WriteLine(mensaje);
                                        Consola_log = Consola_log + mensaje + Environment.NewLine;
                                    }
                                   
                                }
                                else
                                {
                                    string mensaje = "HUBO UN PROBLEMA CON EL NUMERO TRACKID DE FACTURA EN FUNCION EnviarSobreSII() ID_ENVIO: " + listNoEnviados[i] + "TRACK ID: "+ TrackId_str;
                                    Console.WriteLine(mensaje);
                                    Consola_log = Consola_log + mensaje + Environment.NewLine;
                                }
                               



                            }
                            else if (etiqueta == "EnvioBOLETA")
                            {
                                //ES BOLETA
                                //INVOCAR EN LA API EL ENDPONT api/dte/document/enviocliente
                                var httpWebRequest = (HttpWebRequest)WebRequest.Create("http://192.168.1.9:90/api_agrodte/api/dte/document/envioboleta");
                                httpWebRequest.ContentType = "application/json";
                                httpWebRequest.Method = "POST";
                                using (var streamWriter = new StreamWriter(httpWebRequest.GetRequestStream()))
                                {
                                    string path = listRutaXml[0].Replace("\\", "\\\\");

                                    string json_str = @"{
                                ""rut_emisor"": """ + rutEmisor + @""",
                                ""rut_empresa"": """ + rutEmpresa + @""",
                                ""path"": """ + path + @"""
                        }";

                                    streamWriter.Write(json_str);
                                }
                                var httpResponse = (HttpWebResponse)httpWebRequest.GetResponse();
                                string resultApi = "";
                                using (var streamReader = new StreamReader(httpResponse.GetResponseStream()))
                                {
                                    resultApi = streamReader.ReadToEnd();
                                }
                                JObject json = JObject.Parse(resultApi);
                                string respuesta = json.GetValue("respuesta").ToString();
                                string track_id = json.GetValue("track_id").ToString();
                                if (respuesta == "ok")
                                {
                                    string mensaje = "BOLETA ENVIADA TRACKID : " + track_id +" ID ENVIO: "+ listNoEnviados[i];
                                    Console.WriteLine(mensaje);
                                    Consola_log = Consola_log + mensaje + Environment.NewLine;
                                }
                                else
                                {
                                    string mensaje = "HUBO UN ERROR AL TRATAR DE ENVIAR EL SOBRE XML EnviarSobreSII() :" + respuesta + " TRACKID: "+track_id + " ID_ENVIO: "+ listNoEnviados[i];
                                    Console.WriteLine(mensaje);
                                    Consola_log = Consola_log + mensaje + Environment.NewLine;
                                }

                            }
                        }
                        catch (Exception f)
                        {
                            string mensaje = "HUBO UN ERROR AL TRATAR DE ABRIR O ENVIAR EL SOBRE XML EnviarSobreSII() :" + f.Message;
                            Console.WriteLine(mensaje);
                            Consola_log = Consola_log + mensaje + Environment.NewLine;

                            
                        }
                        
                           
                    }
                  

                }
            }

           
            
            try
            {
               
               

                

               
               
            }
            catch (Exception e)
            {
                string mensaje = "HUBO UN ERROR AL VALIDAR EL XML EN FUNCION descargarFacturaCompra() :" + e.Message;
                Console.WriteLine(mensaje);
                Consola_log = Consola_log + mensaje + Environment.NewLine;
                File.WriteAllText(@"..\logerror.txt", e.Message);

            }
        }
            //ENVIAR SOBRES A CLIENTES QUE YA ESTAN REVISADOS POR EL SII
            public static void EnviarSobresClientes()
        {
            try
            {
                //TRAER DE LA BD TODOS LOS SOBRES QUE ESTAN revision_envio_dte = "1" y "envio_cliente_envio_dte" = "0" Y SON DEL DIA
                ConexionBD conexion = new ConexionBD();
                string query_sobres_revisados = "SELECT rutaxml_envio_dte FROM envio_dte WHERE revision_envio_dte = '1' AND envio_cliente_envio_dte = '0' AND fecha_envio_dte > DATE_SUB(NOW(), INTERVAL 24 HOUR)";
                List<string> list_sobres_revisados = conexion.Select(query_sobres_revisados);

                if (list_sobres_revisados.Count == 0)
                {
                    Console.WriteLine("[" + getFecha() + "]: No existen sobres para enviar a cliente");
                    Consola_log = Consola_log + "[" + getFecha() + "]: No existen sobres para enviar" + Environment.NewLine;
                }
                else
                {
                    for (int i = 0; i < list_sobres_revisados.Count; i++)
                    {
                        //ACCEDER A CADA XML PARA SABER EL TIPO DE DTE Y ENVIAR SOLO LOS QUE SON FACTURAS, NOTAS DE CREDITO, NOTAS DE DEBITO Y GUIA DESPACHO.
                        XmlDocument MyDoc = new XmlDocument(); MyDoc.Load(list_sobres_revisados[i]);
                        string etiqueta = MyDoc.LastChild.Name;
                        if (etiqueta == "EnvioDTE")
                        {
                            //SI CUMPLE BUSCAR EL XML DEL CLIENTE AGREGANDO EL PREFIJO A LA RUTA "-cliente.xml"
                            string path_dte_cliente = list_sobres_revisados[i];
                            path_dte_cliente = path_dte_cliente.Remove(path_dte_cliente.Length - 4);
                            path_dte_cliente = path_dte_cliente + "-cliente.xml";

                            //ACCEDEMOS AL XML Y RESCATAMOS EL RUT DEL RECEPTOR
                            XmlDocument doc_cliente = new XmlDocument();
                            doc_cliente.Load(path_dte_cliente);


                            //var doc = new XmlDocument();
                            //doc.LoadXml(path_dte_cliente);
                            var nsmgr = new XmlNamespaceManager(doc_cliente.NameTable);
                            nsmgr.AddNamespace("a", "http://www.sii.cl/SiiDte");
                            var nodes = doc_cliente.SelectNodes("//a:SetDTE/a:DTE/a:Documento/a:Encabezado/a:Receptor/a:RUTRecep", nsmgr);
                            string rut_receptor = nodes[0].InnerText;


                            //INVOCAR EN LA API EL ENDPONT api/dte/document/enviocliente
                            var httpWebRequest2 = (HttpWebRequest)WebRequest.Create("http://192.168.1.9:90/api_agrodte/api/dte/document/enviocliente");
                            //var httpWebRequest2 = (HttpWebRequest)WebRequest.Create("https://localhost:44324/api/dte/document/enviocliente"); //pruebas
                            httpWebRequest2.ContentType = "application/json";
                            httpWebRequest2.Method = "POST";

                            using (var streamWriter = new StreamWriter(httpWebRequest2.GetRequestStream()))
                            {
                                string path = path_dte_cliente.Replace("\\", "\\\\");



                                string json_str = @"{
                                ""rut"": """ + rut_receptor + @""",
                                ""path"": """ + path + @"""
                        }";

                                streamWriter.Write(json_str);
                            }

                            var httpResponse2 = (HttpWebResponse)httpWebRequest2.GetResponse();
                            string result2 = "";
                            using (var streamReader = new StreamReader(httpResponse2.GetResponseStream()))
                            {
                                result2 = streamReader.ReadToEnd();
                            }
                            JObject json2 = JObject.Parse(result2);
                            string respuesta = json2.GetValue("respuesta").ToString();

                            if (respuesta == "ok")
                            {
                                //SETEAR envio_cliente_envio_dte en '1'

                                string strUpdateEstado = "UPDATE envio_dte SET envio_cliente_envio_dte = '1' WHERE rutaxml_envio_dte = '" + list_sobres_revisados[i] + "'";
                                strUpdateEstado = strUpdateEstado.Replace("\\", "\\\\");
                                conexion.Consulta(strUpdateEstado);

                                Console.WriteLine("[" + getFecha() + "]: Sobre enviado con exito al cliente");
                                Consola_log = Consola_log + "[" + getFecha() + "]: Sobre enviado con exito al cliente" + Environment.NewLine;



                            }
                            else
                            {
                                Console.WriteLine("[" + getFecha() + "]: ERROR al enviar sobre al cliente: " +respuesta);
                                Consola_log = Consola_log + "[" + getFecha() + "]: ERROR al enviar sobre al cliente: "+respuesta+ " " + Environment.NewLine;
                            }


                        }
                        else
                        {
                            //EN EL CASO QUE SEA BOLETA EL XML SETEAR EN LA BASE DE DATOS "2" EN EL CAMPO "envio_cliente_envio_dte" ASI NO VUELVE A REVISAR ESTE SOBRE
                            list_sobres_revisados[i] = list_sobres_revisados[i].Replace("\\", "\\\\");
                            string update_envio_cliente = "UPDATE envio_dte SET envio_cliente_envio_dte='2' WHERE rutaxml_envio_dte = '" + list_sobres_revisados[i] + "'";
                            conexion.Consulta(update_envio_cliente);


                        }

                        Thread.Sleep(2000);

                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("[" + getFecha() + "]: Error al enviar sobre cliente: "+ ex.Message);
                Consola_log = Consola_log + "[" + getFecha() + "]: Error al enviar sobre cliente: " + ex.Message + Environment.NewLine;

            }
           

            





        }
        //VERIFICAR VIGENCIA DE LOS CAF
        public static void verificarVigenciaCaf()
        {
            //TRAER LOS RANGOS MAXIMOS DE TODOS LOS CAF ACTIVOS
            List<string> lista_rango_maximo = new List<string>();
            ConexionBD conexion = new ConexionBD();
            lista_rango_maximo =  conexion.Select("SELECT rango_maximo_caf,tipo_documento_caf FROM xml_caf WHERE estado_caf = '1'");

            int j = 1;
            for (int i = 0; i < lista_rango_maximo.Count; i++)
            {
                
                string tipo_dte = "";
                

                if (lista_rango_maximo[j] == "33")
                {
                    tipo_dte = "factura";
                }
                if (lista_rango_maximo[j] == "34")
                {
                    tipo_dte = "factura_exenta";
                }
                if (lista_rango_maximo[j] == "61")
                {
                    tipo_dte = "nota_credito";
                }
                if (lista_rango_maximo[j] == "56")
                {
                    tipo_dte = "nota_debito";
                }
                if (lista_rango_maximo[j] == "52")
                {
                    tipo_dte = "guia_despacho";
                }
                if (lista_rango_maximo[j] == "39")
                {
                    tipo_dte = "boleta";
                }
                if (lista_rango_maximo[j] == "41")
                {
                    tipo_dte = "boleta_exenta";
                }
                List<string> lista_ultimo_folio = new List<string>();
                string consulta_ultimo_folio = "SELECT MAX(folio_" + tipo_dte + ") AS rango_maximo  FROM " + tipo_dte + "";
                lista_ultimo_folio = conexion.Select(consulta_ultimo_folio);

                int diferencia = Int32.Parse(lista_rango_maximo[i]) - Int32.Parse(lista_ultimo_folio[0]);

                var date = DateTime.Now; //ENVIAR CORREOS SOLO EN HORARIO LABORAL

                if (diferencia < 1000 && date.Hour > 9 && date.Hour < 17)
                {
                    mandarEmailCaf(tipo_dte,diferencia);
                }

                j = j + 2;
                i++;
                Console.WriteLine("En el dte "+tipo_dte+" quedan "+diferencia+" folios");

            }
            Console.WriteLine();

            //TRAER EL CORRELATIVO DEL ULTIMO DTE EMITIDO Y COMPARAR CON EL MAXIMO RANGO

            //SI LA DIFERENCIA ES MENOR QUE 200 AVISAR CON MAIL

        }

        public static dynamic obtenerCorreo(Pop3Client client,int i)
        {
            bool intento = true;  
            dynamic correo = null;
            while (intento)
            {

                try
                {
                  
                    correo = client.GetMessage(i);
                    intento = false;
                    return correo;
                }
                catch (Exception e)
                {
                    string mensaje = "ERROR AL CARGAR CORREO descargarFacturaCompra(): " + e.Message + " variable i del correo: " + i;
                    Console.WriteLine(mensaje);
                    Consola_log = Consola_log + mensaje + Environment.NewLine;
                    intento = true;
                    //CON OTRA LIBRERIA INTENTAR DEVOLVER EL CORREO
                    Pop3 cliente = new Pop3();
                    cliente.Connect("mail.agroplastic.cl"); 
                    
                    // or ConnectSSL
                    ConexionBD conexion = new ConexionBD();
                    List<string> respuesta_correo = new List<string>();
                    respuesta_correo = conexion.Select("SELECT mail_intercambio_empresa,pass_intercambio_empresa FROM empresa WHERE id_empresa = 1");
                    

                    cliente.UseBestLogin(respuesta_correo[0], respuesta_correo[1]);
                    var eml = cliente.GetMessageByNumber(i);
                    IMail email = new MailBuilder().CreateFromEml(eml);





                    //ENVIAMOS CORREO
                    SmtpClient mySmtpClient = new SmtpClient("mail.agroplastic.cl");

                    // set smtp-client with basicAuthentication
                    mySmtpClient.UseDefaultCredentials = false;
                    System.Net.NetworkCredential basicAuthenticationInfo = new
                       System.Net.NetworkCredential(MailSistema, PassSistema);
                    mySmtpClient.Credentials = basicAuthenticationInfo;

                    // add from,to mailaddresses
                    MailAddress from = new MailAddress(MailSistema, "AgroDTE");
                    //MailAddress to = new MailAddress("bmcortes@agroplastic.cl, mriquelme@agroplastic.cl", "Benjamin");
                    MailMessage myMail = new MailMessage();
                    myMail.To.Add("bmcortes@agroplastic.cl");
                    myMail.To.Add("mriquelme@agroplastic.cl");
                    myMail.From = from;



                    // add ReplyTo
                    //MailAddress replyTo = new MailAddress("mriquelme@agroplastic.cl");
                    //myMail.ReplyToList.Add(replyTo);

                    // set subject and encoding
                    myMail.Subject = "XML de intercambio";
                    myMail.SubjectEncoding = System.Text.Encoding.UTF8;

                    // set body-message and encoding
                    myMail.Body = "Problema con XML de intercambio: id correo: "+i+" Destinatario: "+email.From[0].Address+" fecha del correo: "+email.Date.ToString()+"";
                    myMail.BodyEncoding = System.Text.Encoding.UTF8;
                    // text or html
                    myMail.IsBodyHtml = true;

                    mySmtpClient.Send(myMail);




                    intento = false;
                    var conteo = email.Attachments.Count();
                    return email;
                    
                    

                }
            }
            return correo;
           
            
        }

       

        //DESCARGAR XML DE SOBRES ENVIADOS AL CORREO DE INTERCAMBIO DTE
        public static void descargarFacturaCompra()
        {
            
          

            


                try
                {
                    //VERIFICAR INTERNET
                    //CHEQUEAR SI HAY CONEXION A INTERNET 
                    string respuestaPing = checkPing("www.google.cl");

                    if (respuestaPing == "Conexion Exitosa")
                    {
                        Pop3Client client = new Pop3Client();
                        client.Connect("mail.agroplastic.cl", 110, false); //UseSSL true or false

                    //TRAER EL CORREO Y PASS DESDE LA BD
                    
                    ConexionBD conexion = new ConexionBD();
                    List<string> respuesta_correo = new List<string>();
                    respuesta_correo = conexion.Select("SELECT mail_intercambio_empresa,pass_intercambio_empresa FROM empresa WHERE id_empresa = 1");

                   
                    client.Authenticate(respuesta_correo[0], respuesta_correo[1]);


                    //BUSCAR LA UID DEL CORREO EN LA BD
                    var uid_correos = client.GetMessageUids();
                    var messageCount = client.GetMessageCount();
                    var topeMensajes = 0;

                    //SI LA CUENTA DE INTERCAMBIO DE CORREOS TIENE MAS DE 5000 CORREOS DEJA UN LIMITE DE ANALIZAR SOLO LA MITAD
                    if (messageCount > 5000)
                    {
                        double resultado = messageCount / 2;
                        topeMensajes = Convert.ToInt32( Math.Round(resultado));
                    }

                        List<Message> allMessages = new List<Message>(messageCount);

                        //RECORRER LOS CORREOS
                        for (int i = messageCount; i > topeMensajes; i--)
                        {
                        Console.WriteLine("Correo id: "+i);
                            //OBTENER EL CORREO
                            dynamic correo = null;

                       
                            correo = obtenerCorreo(client, i);

                        /*try
                        {
                            correo = obtenerCorreo(client, i);
                            //correo = client.GetMessage(i);
                        }
                        catch (Exception r)
                        {

                        continue;
                        }*/


                        //CHEQUEAR SI TIENE XML ADJUNTO

                        //PREGUNTAR EL CORREO EN QUE LIBRERIA VIENE

                        var tipo_libreria = correo.GetType();

                        dynamic archivo_adjunto = null;

                        if (tipo_libreria.Module.Name == "OpenPop.dll")
                        {
                            archivo_adjunto = correo.FindAllAttachments();
                        }
                        else
                        {
                            continue;
                        }
                           

                            if (archivo_adjunto.Count == 0)
                            {
                                //NO TRAE ADJUNTOS

                            }
                            else
                            {
                                int conteo_adjuntos = 0;
                                //TRAE ADJUNTOS,VERIFICAR SI TRAE XML
                                foreach (var attachment in correo.FindAllAttachments())
                                {
                                    if (attachment.FileName.Contains(".xml"))
                                    {
                                        conteo_adjuntos++;

                                        //OBTENER EL UID DEL CORREO
                                        var correo_uid = client.GetMessageUid(i);

                                       
                                        string correo_fecha_sent = correo.Headers.DateSent.ToString();

                                        string correo_fecha = "";
                                        try
                                        {
                                            if (correo_fecha_sent == "01-01-0001 0:00:00")
                                            {

                                                correo_fecha_sent = DateTime.Now.AddHours(3).ToString("dd/MM/yyyy H:mm:ss");
                                            }
                                            DateTime temp = DateTime.ParseExact(correo_fecha_sent, "dd-MM-yyyy H:mm:ss", CultureInfo.InvariantCulture);
                                            //ajustamos zona horaira
                                            DateTime fecha_correo_ajustada = temp.AddHours(-3);
                                            correo_fecha = fecha_correo_ajustada.ToString("yyyy-MM-dd HH:mm:ss");
                                        }
                                        catch (Exception f)
                                        {
                                            Console.WriteLine("HUBO UN ERROR AL PARSEAR LA FECHA DEL CORREO : " + f.Message + " UID CORREO: " + correo_uid);
                                            throw;
                                        }




                                        //BUSCAR LA UID DEL CORREO EN LA BD
                                      
                                        List<string> respuesta = new List<string>();

                                        if (conteo_adjuntos > 1)
                                        {

                                        }
                                        else
                                        {
                                            respuesta = conexion.Select("SELECT uid_correo FROM correo_intercambio WHERE uid_correo = '" + correo_uid + "'");
                                        }


                                   if (respuesta.Count == 0)
                                        {
                                            //NO EXISTE REGISTRO DE UID
                                            //CREAR DIRECTORIO PARA ALMACENAR CORREO

                                            string subject = correo.Headers.Subject.Replace(":", "-");
                                            string directorio = directorio_archivos + @"Correos\" + correo.Headers.From.Address;
                                            string casilla_correo = correo.Headers.From.Address;


                                            if (!Directory.Exists(directorio))
                                            {
                                                try
                                                {
                                                    Directory.CreateDirectory(directorio);
                                                }
                                                catch (Exception e)
                                                {

                                                    Console.WriteLine("HUBO UN ERROR AL CREAR DIRECTORIO DE CORREO: " + e.Message);
                                                }

                                            }

                                            //OBTENER EL BODY DEL CORREO, PUEDE SER TEXTO PLANO O HTML
                                            var plaintext = correo.FindFirstPlainTextVersion();
                                            string texto_correo = "";
                                            string html_correo = "";

                                            if (plaintext != null)
                                            {
                                                texto_correo = plaintext.GetBodyAsText();
                                                int largo_correo = texto_correo.Length;
                                                int max_lenght_correo = 0;
                                                if (largo_correo < 500)
                                                {
                                                    max_lenght_correo = largo_correo;
                                                }
                                                else { max_lenght_correo = 500; }

                                                texto_correo = texto_correo.Substring(0, max_lenght_correo);


                                            }
                                            else
                                            {
                                                var html = correo.FindFirstHtmlVersion();

                                                if (html != null)
                                                {
                                                    html_correo = html.GetBodyAsText();
                                                }
                                            }

                                            string filePath = Path.Combine(directorio, correo_uid + attachment.FileName.Replace("/", "_"));

                                            FileStream Stream = new FileStream(filePath, FileMode.Create);
                                            BinaryWriter BinaryStream = new BinaryWriter(Stream);
                                            BinaryStream.Write(attachment.Body);
                                            BinaryStream.Close();

                                            //VERIFICAR QUE TIPO DE DTE ES
                                            XmlDocument xml_file = new XmlDocument();
                                            try
                                            {

                                                xml_file.Load(filePath);
                                            }
                                            catch (Exception e)
                                            {
                                                //ARMAR POSIBLE RESPUESTA (ACUSE RECIBO)
                                                string path_api2 = "";
                                                if (es_produccion)
                                                {
                                                    path_api2 = "http://192.168.1.9:90/api_agrodte/api/dte/EmitirAcuseRecibo";
                                                }
                                                else
                                                {
                                                    path_api2 = "http://192.168.1.9:90/api_agrodte/api/dte/EmitirAcuseRecibo";
                                                    //path_api2 = "https://localhost:44324/api/dte/EmitirAcuseRecibo";
                                                }
                                                var httpWebRequest2 = (HttpWebRequest)WebRequest.Create(path_api2);
                                                httpWebRequest2.ContentType = "application/json";
                                                httpWebRequest2.Method = "POST";

                                                using (var streamWriter = new StreamWriter(httpWebRequest2.GetRequestStream()))
                                                {
                                                    string path = filePath.Replace("\\", "\\\\");



                                                    string json_str = "{" +
                                                        "\"datosAcuseRecibo\":{" +
                                                                   "\"PathXML\":\"" + path + "\"," +
                                                                  "\"RespuestaSobre\":\"RecepcionEnvio-Ilegible\"," +
                                                                  "\"respuestaDte\":[]," +
                                                                    "\"rutEmpresaResponde\":\"" + RutEmisor + "\"," +
                                                                    "\"prefixPathXML\": \"" + correo_uid + "\"" +
                                                                    "}" +
                                                                  "}";

                                                    streamWriter.Write(json_str);
                                                }

                                                var httpResponse2 = (HttpWebResponse)httpWebRequest2.GetResponse();
                                                string result2 = "";
                                                using (var streamReader = new StreamReader(httpResponse2.GetResponseStream()))
                                                {
                                                    result2 = streamReader.ReadToEnd();
                                                }
                                                JObject json2 = JObject.Parse(result2);
                                                string statusCode2 = json2.GetValue("statusCode").ToString();

                                                if (statusCode2 == "500")
                                                {
                                                    //ERROR
                                                    string mensaje = json2.GetValue("message").ToString();
                                                    Console.WriteLine(mensaje);

                                                }

                                                break;
                                            }


                                            //HACER UN IF PARA GUARDAR ACUSES DE RECIBO O GUARDAR DTE
                                            // <RESPUESTADTE> <DTE>
                                            var dte = xml_file.GetElementsByTagName("DTE");
                                            var respuesta_dte = xml_file.GetElementsByTagName("RespuestaDTE");
                                            var envio_recibos = xml_file.GetElementsByTagName("EnvioRecibos");
                                            var RutReceptor_Caratula = xml_file.GetElementsByTagName("RutReceptor");
                                            bool errorAcuseRecibo = false;
                                            string respuestaSobre = "";
                                            List<string> respuestaDte = new List<string>();


                                            //RUT DE LA CARATULA NO CORRESPONDE CON RUT EMPRESA RECEPTOR
                                            if (RutReceptor_Caratula.Count != 0)
                                            {
                                                if (RutEmisor != RutReceptor_Caratula[0].InnerText && dte.Count > 0)
                                                {
                                                    /*{
                                                        "datosAcuseRecibo": {
                                                            "PathXML": "C:\\inetpub\\wwwroot\\api_agrodte\\AgroDTE_Archivos\\Correos\\EnvioDTE.xml",
                                                            "RespuestaSobre": "RecepcionEnvio-Rut",
                                                            "respuestaDte": "",
                                                            "rutEmpresaResponde": "76958430-7"
                                                        }
                                                    }*/
                                                    respuestaSobre = "RecepcionEnvio-Rut";
                                                    errorAcuseRecibo = true;
                                                }
                                                else
                                                {
                                                    respuestaSobre = "RecepcionEnvio-Conforme";
                                                }

                                            }


                                            //SI EXISTE ETIQUETA <DTE> ES UN DTE DE COMPRA, SI EXISTE <RespuestaDTE> es un acuse recibo
                                            if (dte.Count != 0 && !errorAcuseRecibo)
                                            {

                                                //VALIDAR XML CON SCHEMA

                                                string filename = filePath;
                                                string respuestaSchema = "";
                                                string schemaFileName = schemaFileName = @"C:\inetpub\wwwroot\api_agrodte\AgroDTE_Archivos\Schemas\DTEs\EnvioDTE_v10.xsd";
                                                try
                                                {
                                                    string path_servicio_validar_xml = "";
                                                    if (es_produccion)
                                                    {
                                                        path_servicio_validar_xml = "http://192.168.1.9:90/WebServiceValidarXML/ValidarXML.asmx/ValidarXml?xmlFilename=" + filename + "&schemaFilename=" + schemaFileName;
                                                    }
                                                    else
                                                    {
                                                        path_servicio_validar_xml = "http://localhost:81/WebServiceValidarXML/ValidarXML.asmx/ValidarXml?xmlFilename=" + filename + "&schemaFilename=" + schemaFileName;
                                                        //path_servicio_validar_xml = "http://192.168.1.9:90/WebServiceValidarXML/ValidarXML.asmx/ValidarXml?xmlFilename=" + filename + "&schemaFilename=" + schemaFileName;
                                                    }

                                                    WebRequest request = WebRequest.Create(path_servicio_validar_xml);
                                                    request.Method = "GET";
                                                    WebResponse response = request.GetResponse();

                                                    using (var reader = new StreamReader(response.GetResponseStream()))
                                                    {
                                                        respuestaSchema = reader.ReadToEnd(); // do something fun...
                                                    }
                                                }
                                                catch (Exception e)
                                                {
                                                    string mensaje = "HUBO UN ERROR AL VALIDAR EL XML EN FUNCION descargarFacturaCompra() :" + e.Message;
                                                    Console.WriteLine(mensaje);
                                                    Consola_log = Consola_log + mensaje + Environment.NewLine;
                                                    File.WriteAllText(@"..\logerror.txt", e.Message);

                                                }

                                                //ESTA RESPUESTA ES UN XML PERO ES UN STRING, POR LO TANTO PARSEAMOS DE STRING A XML
                                                XmlDocument xmlDoc = new XmlDocument();
                                                xmlDoc.LoadXml(respuestaSchema);

                                                XmlNodeList elemlist = xmlDoc.GetElementsByTagName("string");
                                                string resultadoSchema = elemlist[0].InnerXml;
                                                string mensajeSchema = elemlist[1].InnerXml;
                                                string errorSchemaFlag = "False";


                                                if (resultadoSchema == "False")
                                                {
                                                    respuestaSobre = "RecepcionEnvio-Schema";

                                                    //CAMBIAR EL NOMBRE DEL XML Y AGREGARLE UN "ERROR" EN EL NOMBRE
                                                    string filenameError = filename.Substring(0, filename.Length - 4);
                                                    try
                                                    {
                                                        //System.IO.File.Move(filename, filenameError + "ERRORSCHEMA.xml");
                                                    }
                                                    catch (Exception e)
                                                    {


                                                    }

                                                    string mensaje = "[" + getFecha() + "]: DTE RECIBIDO XML Invalido: ID: " + correo_uid + " Mensaje del Schema: " + mensajeSchema;
                                                    Console.WriteLine(mensaje);
                                                    Consola_log = Consola_log + mensaje + Environment.NewLine;

                                                    //SE SETEA EN "True" para que grabe igual el xml en caso de venir sin alguna etiqueta
                                                    resultadoSchema = "True";
                                                    errorSchemaFlag = "True";


                                                }


                                                //parsear xml para obtener la respuesta de la etiqueta string "Document is valid"
                                                if (resultadoSchema == "True")
                                                {
                                                    if (resultadoSchema == "True" && errorSchemaFlag == "True")
                                                    {
                                                        respuestaSchema = "False";
                                                    }
                                                    try
                                                    {
                                                        //NO HAY ERRORES

                                                        if (conteo_adjuntos > 1)
                                                        {

                                                        }
                                                        else
                                                        {
                                                            directorio = directorio.Replace("\\", "\\\\");
                                                            html_correo = html_correo.Replace("'", "\"");
                                                            conexion.Consulta("INSERT INTO correo_intercambio (uid_correo,cuerpo_correo_texto,cuerpo_correo_html,ruta_correo,fecha_correo,casilla_correo) VALUES ('" + correo_uid + "','" + texto_correo + "','" + html_correo + "','" + directorio + "','" + correo_fecha + "','" + casilla_correo + "')");
                                                        }



                                                        //var nodo_tipodte = xml_file.GetElementsByTagName("TipoDTE");
                                                        string tipo_dte = "";
                                                        string folio = "";
                                                        string fchemis = "";
                                                        string rutemis = "";
                                                        string rznsocemisor = "";
                                                        string cmnaorigenemisor = "";
                                                        string mnttotal = "";
                                                        string detalles = "";
                                                        string referencia_folio = "";
                                                        string referencia_tipodte = "";
                                                        string query = "";
                                                        string rutrecep = "";

                                                        respuestaSobre = "RecepcionEnvio-Conforme";
                                                        int contador_repetidos_dte = 0;

                                                        for (int j = 0; j < dte.Count; j++)
                                                        {
                                                            bool errorDTE = false;



                                                            for (int l = 0; l < dte[j].ChildNodes.Count; l++)
                                                            {
                                                                XmlNode nodo_documento = dte[j].ChildNodes[l];




                                                                if (nodo_documento.Name == "Documento")
                                                                {
                                                                    for (int o = 0; o < nodo_documento.ChildNodes.Count; o++)
                                                                    {
                                                                        if (nodo_documento.ChildNodes[o].Name == "Encabezado")
                                                                        {
                                                                            //BUSCAR ETIQUETAS DENTRO DEL <Encabezado>
                                                                            var nodo_encabezado = nodo_documento.ChildNodes[o];

                                                                            for (int p = 0; p < nodo_encabezado.ChildNodes.Count; p++)
                                                                            {
                                                                                //BUSCAR NODOS DENTRO DE <idDoc>
                                                                                if (nodo_encabezado.ChildNodes[p].Name == "IdDoc")
                                                                                {
                                                                                    var nodo_idDoc = nodo_encabezado.ChildNodes[p];

                                                                                    for (int u = 0; u < nodo_idDoc.ChildNodes.Count; u++)
                                                                                    {

                                                                                        if (nodo_idDoc.ChildNodes[u].Name == "TipoDTE")
                                                                                        {
                                                                                            tipo_dte = nodo_idDoc.ChildNodes[u].InnerText;
                                                                                        }
                                                                                        else if (nodo_idDoc.ChildNodes[u].Name == "Folio")
                                                                                        {
                                                                                            folio = nodo_idDoc.ChildNodes[u].InnerText;
                                                                                        }
                                                                                        else if (nodo_idDoc.ChildNodes[u].Name == "FchEmis")
                                                                                        {
                                                                                            fchemis = nodo_idDoc.ChildNodes[u].InnerText;
                                                                                        }

                                                                                    }

                                                                                }
                                                                                //BUSCAR NODOS DENTRO DE <Emisor>
                                                                                else if (nodo_encabezado.ChildNodes[p].Name == "Emisor")
                                                                                {
                                                                                    var nodo_Emisor = nodo_encabezado.ChildNodes[p];

                                                                                    for (int m = 0; m < nodo_Emisor.ChildNodes.Count; m++)
                                                                                    {
                                                                                        if (nodo_Emisor.ChildNodes[m].Name == "RUTEmisor")
                                                                                        {
                                                                                            rutemis = nodo_Emisor.ChildNodes[m].InnerText;
                                                                                        }
                                                                                        else if (nodo_Emisor.ChildNodes[m].Name == "RznSoc")
                                                                                        {
                                                                                            rznsocemisor = nodo_Emisor.ChildNodes[m].InnerText;
                                                                                        }
                                                                                        else if (nodo_Emisor.ChildNodes[m].Name == "CmnaOrigen")
                                                                                        {
                                                                                            cmnaorigenemisor = nodo_Emisor.ChildNodes[m].InnerText;
                                                                                        }
                                                                                    }
                                                                                }
                                                                                else if (nodo_encabezado.ChildNodes[p].Name == "Receptor")
                                                                                {
                                                                                    var nodo_Receptor = nodo_encabezado.ChildNodes[p];
                                                                                    for (int r = 0; r < nodo_Receptor.ChildNodes.Count; r++)
                                                                                    {
                                                                                        if (nodo_Receptor.ChildNodes[r].Name == "RUTRecep")
                                                                                        {
                                                                                            rutrecep = nodo_Receptor.ChildNodes[r].InnerText;
                                                                                            if (RutEmisor != rutrecep)
                                                                                            {
                                                                                                respuestaDte.Add("RecepcionDTE-Rut Receptor");
                                                                                                errorDTE = true;
                                                                                            }


                                                                                        }
                                                                                    }

                                                                                }

                                                                                //BUSCAR NODOS DENTRO DE <Totales>
                                                                                else if (nodo_encabezado.ChildNodes[p].Name == "Totales")
                                                                                {
                                                                                    var nodo_Totales = nodo_encabezado.ChildNodes[p];

                                                                                    for (int m = 0; m < nodo_Totales.ChildNodes.Count; m++)
                                                                                    {
                                                                                        if (nodo_Totales.ChildNodes[m].Name == "MntTotal")
                                                                                        {
                                                                                            mnttotal = nodo_Totales.ChildNodes[m].InnerText;
                                                                                        }

                                                                                    }
                                                                                }



                                                                            }

                                                                        }
                                                                        //BUSCAR ETIQUETAS DENTRO DEL <Detalle>
                                                                        else if (nodo_documento.ChildNodes[o].Name == "Detalle")
                                                                        {
                                                                            var nodo_detalle = nodo_documento.ChildNodes[o];
                                                                            for (int e = 0; e < nodo_detalle.ChildNodes.Count; e++)
                                                                            {
                                                                                if (nodo_detalle.ChildNodes[e].Name == "NmbItem")
                                                                                {
                                                                                    detalles = detalles + nodo_detalle.ChildNodes[e].InnerText + ";";
                                                                                }
                                                                            }

                                                                        }
                                                                        //BUSCAR ETIQUETAS DENTRO DEL <Referencia>
                                                                        else if (nodo_documento.ChildNodes[o].Name == "Referencia")
                                                                        {
                                                                            var nodo_referencia = nodo_documento.ChildNodes[o];
                                                                            for (int e = 0; e < nodo_referencia.ChildNodes.Count; e++)
                                                                            {
                                                                                if (nodo_referencia.ChildNodes[e].Name == "TpoDocRef")
                                                                                {
                                                                                    referencia_tipodte = nodo_referencia.ChildNodes[e].InnerText;
                                                                                }
                                                                                else if (nodo_referencia.ChildNodes[e].Name == "FolioRef")
                                                                                {
                                                                                    referencia_folio = nodo_referencia.ChildNodes[e].InnerText;
                                                                                }
                                                                            }
                                                                        }



                                                                    }

                                                                }
                                                            }

                                                            //INSERTAR DTE DEPENDIENDO DEL TIPO

                                                            //Eliminar el ultimo ; del detalle string
                                                            if (detalles.Length == 0)
                                                            {

                                                            }
                                                            else
                                                            {
                                                                detalles = detalles.Remove(detalles.Length - 1);
                                                            }


                                                            if (j == 0)
                                                            {
                                                                filePath = filePath.Replace("\\", "\\\\");
                                                            }


                                                            //VERIFICAR RUT RECEPTOR CORRECTO
                                                            int errorSchema = 0;
                                                            if (errorSchemaFlag == "True")
                                                            {
                                                                errorSchema = 2;
                                                            }

                                                            if (errorSchemaFlag == "False")
                                                            {
                                                                errorSchema = 1;
                                                            }
                                                            switch (tipo_dte)
                                                            {
                                                                case "33":
                                                                    try
                                                                    {

                                                                        string query_verificar = "SELECT folio_factura_compra FROM factura_compra WHERE folio_factura_compra = '" + folio + "' AND rutemis_factura_compra = '" + rutemis + "'";

                                                                        List<string> lista_verificar = conexion.Select(query_verificar);
                                                                        if (lista_verificar.Count == 0)
                                                                        {
                                                                            query = "INSERT INTO factura_compra (folio_factura_compra,uid_correo,fchemis_factura_compra,rutemis_factura_compra," +
                                                                                                                                            "rznsocemis_factura_compra,cmnaorigen_factura_compra,mnttotal_factura_compra,detalle_factura_compra,folioref_factura_compra,tipo_dteref_factura_compra,ubicacion_factura_compra,estado_schema_factura_compra)" +
                                                                                                                                            "VALUES ('" + folio + "', '" + correo_uid + "', '" + fchemis + "', '" + rutemis + "', '" + rznsocemisor + "', '" + cmnaorigenemisor + "', '" + mnttotal + "'," +
                                                                                                                                            " '" + detalles + "', '" + referencia_folio + "','" + referencia_tipodte + "', '" + filePath + "','" + errorSchema + "')";

                                                                            conexion.Consulta(query);
                                                                            detalles = "";
                                                                            respuestaDte.Add("RecepcionDTE-OK");
                                                                        }
                                                                        else
                                                                        {
                                                                            //AUMENTA EL CONTADOR
                                                                            contador_repetidos_dte++;
                                                                            if (!errorDTE)
                                                                            {
                                                                                respuestaDte.Add("RecepcionDTE-Repetido");
                                                                            }
                                                                            else
                                                                            {

                                                                            }


                                                                        }



                                                                    }
                                                                    catch (Exception e)
                                                                    {
                                                                        Console.WriteLine("HUBO UN PROBLEMA AL LEER XML DE PROVEEDOR: " + correo_uid + " MENSAJE: " + e.Message);

                                                                    }



                                                                    break;

                                                                case "34":
                                                                    try
                                                                    {
                                                                        string query_verificar = "SELECT folio_factura_exenta_compra FROM factura_exenta_compra WHERE folio_factura_exenta_compra = '" + folio + "' AND rutemis_factura_exenta_compra = '" + rutemis + "'";

                                                                        List<string> lista_verificar = conexion.Select(query_verificar);

                                                                        if (lista_verificar.Count == 0)
                                                                        {
                                                                            query = "INSERT INTO factura_exenta_compra (folio_factura_exenta_compra,uid_correo,fchemis_factura_exenta_compra,rutemis_factura_exenta_compra," +
                                                                       "rznsocemis_factura_exenta_compra,cmnaorigen_factura_exenta_compra,mnttotal_factura_exenta_compra,detalle_factura_exenta_compra,folioref_factura_exenta_compra,tipo_dteref_factura_exenta_compra,ubicacion_factura_exenta_compra,estado_schema_factura_exenta_compra)" +
                                                                       "VALUES ('" + folio + "', '" + correo_uid + "', '" + fchemis + "', '" + rutemis + "', '" + rznsocemisor + "', '" + cmnaorigenemisor + "', '" + mnttotal + "'," +
                                                                       " '" + detalles + "', '" + referencia_folio + "','" + referencia_tipodte + "', '" + filePath + "','" + errorSchema + "')";

                                                                            conexion.Consulta(query);
                                                                            detalles = "";
                                                                            respuestaDte.Add("RecepcionDTE-OK");
                                                                        }
                                                                        else
                                                                        {
                                                                            //AUMENTA EL CONTADOR
                                                                            contador_repetidos_dte++;
                                                                            if (!errorDTE)
                                                                            {
                                                                                respuestaDte.Add("RecepcionDTE-Repetido");
                                                                            }
                                                                            else
                                                                            {

                                                                            }
                                                                        }




                                                                    }
                                                                    catch (Exception e)
                                                                    {
                                                                        Console.WriteLine("HUBO UN PROBLEMA AL LEER XML DE PROVEEDOR: " + correo_uid + " MENSAJE: " + e.Message);

                                                                    }


                                                                    break;

                                                                case "61":
                                                                    try
                                                                    {
                                                                        string query_verificar = "SELECT folio_nota_credito_compra FROM nota_credito_compra WHERE folio_nota_credito_compra = '" + folio + "' AND rutemis_nota_credito_compra = '" + rutemis + "'";

                                                                        List<string> lista_verificar = conexion.Select(query_verificar);
                                                                        if (lista_verificar.Count == 0)
                                                                        {



                                                                            query = "INSERT INTO nota_credito_compra (folio_nota_credito_compra,uid_correo,fchemis_nota_credito_compra,rutemis_nota_credito_compra," +
                                                                            "rznsocemis_nota_credito_compra,cmnaorigen_nota_credito_compra,mnttotal_nota_credito_compra,detalle_nota_credito_compra,folioref_nota_credito_compra,tipo_dteref_nota_credito_compra,ubicacion_nota_credito_compra,estado_schema_nota_credito_compra)" +
                                                                            "VALUES ('" + folio + "', '" + correo_uid + "', '" + fchemis + "', '" + rutemis + "', '" + rznsocemisor + "', '" + cmnaorigenemisor + "', '" + mnttotal + "'," +
                                                                            " '" + detalles + "', '" + referencia_folio + "','" + referencia_tipodte + "', '" + filePath + "','" + errorSchema + "')";

                                                                            conexion.Consulta(query);
                                                                            detalles = "";
                                                                            respuestaDte.Add("RecepcionDTE-OK");
                                                                        }
                                                                        else
                                                                        {
                                                                            //AUMENTA EL CONTADOR
                                                                            contador_repetidos_dte++;
                                                                            if (!errorDTE)
                                                                            {
                                                                                respuestaDte.Add("RecepcionDTE-Repetido");
                                                                            }
                                                                            else
                                                                            {

                                                                            }
                                                                        }

                                                                    }
                                                                    catch (Exception e)
                                                                    {
                                                                        Console.WriteLine("HUBO UN PROBLEMA AL LEER XML DE PROVEEDOR: " + correo_uid + " MENSAJE: " + e.Message);

                                                                    }


                                                                    break;

                                                                case "56":
                                                                    try
                                                                    {
                                                                        string query_verificar = "SELECT folio_nota_debito_compra FROM nota_debito_compra WHERE folio_nota_debito_compra = '" + folio + "' AND rutemis_nota_debito_compra = '" + rutemis + "'";

                                                                        List<string> lista_verificar = conexion.Select(query_verificar);

                                                                        if (lista_verificar.Count == 0)
                                                                        {
                                                                            query = "INSERT INTO nota_debito_compra (folio_nota_debito_compra,uid_correo,fchemis_nota_debito_compra,rutemis_nota_debito_compra," +
                                                                                                                                           "rznsocemis_nota_debito_compra,cmnaorigen_nota_debito_compra,mnttotal_nota_debito_compra,detalle_nota_debito_compra,folioref_nota_debito_compra,tipo_dteref_nota_debito_compra,ubicacion_nota_debito_compra,estado_schema_nota_debito_compra)" +
                                                                                                                                           "VALUES ('" + folio + "', '" + correo_uid + "', '" + fchemis + "', '" + rutemis + "', '" + rznsocemisor + "', '" + cmnaorigenemisor + "', '" + mnttotal + "'," +
                                                                                                                                           " '" + detalles + "', '" + referencia_folio + "','" + referencia_tipodte + "', '" + filePath + "','" + errorSchema + "')";

                                                                            conexion.Consulta(query);

                                                                            detalles = "";
                                                                            respuestaDte.Add("RecepcionDTE-OK");
                                                                        }
                                                                        else
                                                                        {
                                                                            //AUMENTA EL CONTADOR
                                                                            contador_repetidos_dte++;
                                                                            if (!errorDTE)
                                                                            {
                                                                                respuestaDte.Add("RecepcionDTE-Repetido");
                                                                            }
                                                                            else
                                                                            {

                                                                            }

                                                                        }



                                                                    }
                                                                    catch (Exception e)
                                                                    {
                                                                        Console.WriteLine("HUBO UN PROBLEMA AL LEER XML DE PROVEEDOR: " + correo_uid + " MENSAJE: " + e.Message);

                                                                    }


                                                                    break;

                                                                case "52":
                                                                    try
                                                                    {
                                                                        string query_verificar = "SELECT folio_guia_despacho_compra FROM guia_despacho_compra WHERE folio_guia_despacho_compra = '" + folio + "' AND rutemis_guia_despacho_compra = '" + rutemis + "'";

                                                                        List<string> lista_verificar = conexion.Select(query_verificar);
                                                                        if (lista_verificar.Count == 0)
                                                                        {

                                                                            query = "INSERT INTO guia_despacho_compra (folio_guia_despacho_compra,uid_correo,fchemis_guia_despacho_compra,rutemis_guia_despacho_compra," +
                                                                            "rznsocemis_guia_despacho_compra,cmnaorigen_guia_despacho_compra,mnttotal_guia_despacho_compra,detalle_guia_despacho_compra,folioref_guia_despacho_compra,tipo_dteref_guia_despacho_compra,ubicacion_guia_despacho_compra,estado_schema_guia_despacho_compra)" +
                                                                            "VALUES ('" + folio + "', '" + correo_uid + "', '" + fchemis + "', '" + rutemis + "', '" + rznsocemisor + "', '" + cmnaorigenemisor + "', '" + mnttotal + "'," +
                                                                            " '" + detalles + "', '" + referencia_folio + "', '" + referencia_tipodte + "','" + filePath + "','" + errorSchema + "')";

                                                                            conexion.Consulta(query);
                                                                            detalles = "";
                                                                            respuestaDte.Add("RecepcionDTE-OK");
                                                                        }
                                                                        else
                                                                        {
                                                                            //AUMENTA EL CONTADOR
                                                                            contador_repetidos_dte++;
                                                                            if (!errorDTE)
                                                                            {
                                                                                respuestaDte.Add("RecepcionDTE-Repetido");
                                                                            }
                                                                            else
                                                                            {

                                                                            }
                                                                        }
                                                                    }
                                                                    catch (Exception e)
                                                                    {
                                                                        Console.WriteLine("HUBO UN PROBLEMA AL LEER XML DE PROVEEDOR: " + correo_uid + " MENSAJE: " + e.Message);

                                                                    }
                                                                    break;
                                                                default:
                                                                    break;
                                                            }

                                                            string mensaje2 = "[" + getFecha() + "]: DTE RECIBIDO XML ID Correo : " + correo_uid + " Folio: " + folio + " Razon Social: " + rznsocemisor;
                                                            Console.WriteLine(mensaje2);
                                                            Consola_log = Consola_log + mensaje2 + Environment.NewLine;

                                                        }
                                                        //VERIFICAR SI EL SOBRE ES REPETIDO
                                                        if (dte.Count == contador_repetidos_dte)
                                                        {
                                                            respuestaSobre = "RecepcionEnvio-Conforme";

                                                        }
                                                        else
                                                        {
                                                            respuestaSobre = "RecepcionEnvio-Conforme";
                                                        }


                                                    }
                                                    catch (Exception ex)
                                                    {

                                                        string mensaje = "HUBO UN PROBLEMA AL INSERTAR DATOS DE DTE DE COMPRA FUNCION descargarFacturaCompra():  " + ex.Message;
                                                        Consola_log = Consola_log + mensaje + Environment.NewLine;
                                                        Console.WriteLine(mensaje);
                                                    }



                                                }





                                            }
                                            else if (respuesta_dte.Count != 0 && !errorAcuseRecibo)
                                            {
                                                //VALIDAR XML DE RESPUESTA CON SCHEMA

                                                string filename = filePath;
                                                string respuestaSchema = "";
                                                string schemaFileName = schemaFileName = @"C:\inetpub\wwwroot\api_agrodte\AgroDTE_Archivos\Schemas\DTEs\RespuestaEnvioDTE_v10.xsd";
                                                try
                                                {
                                                    string path_servicio_validar_xml = "";
                                                    if (es_produccion)
                                                    {
                                                        path_servicio_validar_xml = "http://192.168.1.9:90/WebServiceValidarXML/ValidarXML.asmx/ValidarXml?xmlFilename=" + filename + "&schemaFilename=" + schemaFileName;

                                                    }
                                                    else
                                                    {

                                                        path_servicio_validar_xml = "http://localhost:81/WebServiceValidarXML/ValidarXML.asmx/ValidarXml?xmlFilename=" + filename + "&schemaFilename=" + schemaFileName;
                                                    }


                                                    WebRequest request = WebRequest.Create(path_servicio_validar_xml);
                                                    request.Method = "GET";
                                                    WebResponse response = request.GetResponse();

                                                    using (var reader = new StreamReader(response.GetResponseStream()))
                                                    {
                                                        respuestaSchema = reader.ReadToEnd(); // do something fun...
                                                    }
                                                }
                                                catch (Exception e)
                                                {

                                                    File.WriteAllText(@"..\logerror.txt", e.Message);
                                                }

                                                //ESTA RESPUESTA ES UN XML PERO ES UN STRING, POR LO TANTO PARSEAMOS DE STRING A XML
                                                XmlDocument xmlDoc = new XmlDocument();
                                                xmlDoc.LoadXml(respuestaSchema);

                                                XmlNodeList elemlist = xmlDoc.GetElementsByTagName("string");
                                                string resultadoSchema = elemlist[0].InnerXml;
                                                string mensajeSchema = elemlist[1].InnerXml;

                                                //parsear xml para obtener la respuesta de la etiqueta string "Document is valid"
                                                if (resultadoSchema == "True")
                                                {
                                                    //NO HAY ERRORES
                                                    //Console.WriteLine("XML Valido");

                                                }
                                                else
                                                {
                                                    //HAY ERRORES EN XML
                                                    //CAMBIAR EL NOMBRE DEL XML Y AGREGARLE UN "ERROR" EN EL NOMBRE
                                                    string filenameError = filename.Substring(0, filename.Length - 4);
                                                    System.IO.File.Move(filename, filenameError + "ERRORSCHEMA.xml");
                                                    Console.WriteLine("XML Invalido:" + mensajeSchema);
                                                }

                                                //CORRESPONDE A UNA RESPUESTA EN XML
                                                if (conteo_adjuntos > 1)
                                                {

                                                }
                                                else
                                                {
                                                   directorio = directorio.Replace("\\", "\\\\");
                                                html_correo = html_correo.Replace("'", "\"");
                                                conexion.Consulta("INSERT INTO correo_intercambio (uid_correo,cuerpo_correo_texto,cuerpo_correo_html,ruta_correo,fecha_correo,casilla_correo) VALUES ('" + correo_uid + "','" + texto_correo + "','" + html_correo + "','" + directorio + "','" + correo_fecha + "','" + casilla_correo + "')");
                                                }

                                                //VARIABLES PARA GUARDAR EN LA BD DE <RecepcionEnvio>
                                                long CodEnvio = 0;
                                                string FchRecep = "";
                                                string horaRecep = ""; //Este se obtiene a partir FchRecep
                                                string RutEmisor = "";
                                                int EstadoRecepEnv = 0;
                                                string RecepEnvGlosa = "";
                                                //VARIABLES PARA GUARDAR EN LA BD DE <RecepcionDTE>
                                                int tipo_dte = 0;
                                                long folio_dte = 0;
                                                int estado_dte = 0;
                                                string glosa_dte = "";


                                                for (int j = 0; j < respuesta_dte.Count; j++)
                                                {
                                                    XmlNodeList nodos_respuesta = respuesta_dte[j].ChildNodes;

                                                    for (int k = 0; k < nodos_respuesta.Count; k++)
                                                    {

                                                        if (nodos_respuesta[k].Name == "Resultado")
                                                        {
                                                            XmlNodeList nodos_resultado = nodos_respuesta[k].ChildNodes;

                                                            for (int m = 0; m < nodos_resultado.Count; m++)
                                                            {
                                                                if (nodos_resultado[m].Name == "RecepcionEnvio")
                                                                {
                                                                    // ES UNA RESPUESTA DE ACEPTACION DE ENVIO DTE
                                                                    List<string> list_codigo_acuse_recibo = conexion.Select("SELECT MAX(id_acuse_recibo_cliente) FROM acuse_recibo_cliente");
                                                                    int ultimo_id = int.Parse(list_codigo_acuse_recibo[0]) + 1;
                                                                    string insertar_id_acuse_recibo = "INSERT INTO acuse_recibo_cliente (id_acuse_recibo_cliente) VALUES ('" + ultimo_id + "')";
                                                                    conexion.Consulta(insertar_id_acuse_recibo);

                                                                    XmlNodeList nodos_recepcion_envio = nodos_resultado[m].ChildNodes;

                                                                    for (int n = 0; n < nodos_recepcion_envio.Count; n++)
                                                                    {
                                                                        if (nodos_recepcion_envio[n].Name == "CodEnvio")
                                                                        {
                                                                            CodEnvio = long.Parse(nodos_recepcion_envio[n].InnerText);
                                                                        }

                                                                        if (nodos_recepcion_envio[n].Name == "FchRecep")
                                                                        {
                                                                            string fecha = nodos_recepcion_envio[n].InnerText;
                                                                            char delimitador = 'T';
                                                                            string[] fecha_separada = fecha.Split(delimitador);
                                                                            FchRecep = fecha_separada[0];
                                                                            horaRecep = fecha_separada[1];
                                                                        }
                                                                        if (nodos_recepcion_envio[n].Name == "RutEmisor")
                                                                        {
                                                                            RutEmisor = nodos_recepcion_envio[n].InnerText;
                                                                        }
                                                                        if (nodos_recepcion_envio[n].Name == "EstadoRecepEnv")
                                                                        {
                                                                            EstadoRecepEnv = int.Parse(nodos_recepcion_envio[n].InnerText);
                                                                        }
                                                                        if (nodos_recepcion_envio[n].Name == "RecepEnvGlosa")
                                                                        {
                                                                            RecepEnvGlosa = nodos_recepcion_envio[n].InnerText;
                                                                        }

                                                                        if (nodos_recepcion_envio[n].Name == "RecepcionDTE")
                                                                        {
                                                                            XmlNodeList nodos_recepcionDTE = nodos_recepcion_envio[n].ChildNodes;
                                                                            for (int h = 0; h < nodos_recepcionDTE.Count; h++)
                                                                            {
                                                                                if (nodos_recepcionDTE[h].Name == "TipoDTE")
                                                                                {
                                                                                    tipo_dte = int.Parse(nodos_recepcionDTE[h].InnerText);
                                                                                }
                                                                                if (nodos_recepcionDTE[h].Name == "Folio")
                                                                                {
                                                                                    folio_dte = int.Parse(nodos_recepcionDTE[h].InnerText);
                                                                                }
                                                                                if (nodos_recepcionDTE[h].Name == "EstadoRecepDTE")
                                                                                {
                                                                                    estado_dte = int.Parse(nodos_recepcionDTE[h].InnerText);
                                                                                }
                                                                                if (nodos_recepcionDTE[h].Name == "RecepDTEGlosa")
                                                                                {
                                                                                    glosa_dte = nodos_recepcionDTE[h].InnerText;
                                                                                }

                                                                            }

                                                                            //INSERTAR DATOWS RECEPCIONDTE
                                                                            conexion.Consulta("INSERT INTO acuso_recibo_dte_cliente (id_acuse_recibo_cliente_fk,tipodte_acuse_recibo_cliente_dte,folio_acuse_recibo_cliente_dte,estado_recep_acuse_recibo_cliente,glosa_estado_acuse_recibo_cliente) VALUES ('" + ultimo_id + "','" + tipo_dte + "','" + folio_dte + "','" + estado_dte + "','" + glosa_dte + "')");
                                                                            //UPDATEAR EL FK EN EL DTE CORRESPONDIENTE DEPENDIENDO DEL TIPO DTE Y EL FOLIO
                                                                            string tabla = "";
                                                                            if (tipo_dte == 33) { tabla = "factura"; }
                                                                            if (tipo_dte == 61) { tabla = "nota_credito"; }
                                                                            if (tipo_dte == 56) { tabla = "nota_debito"; }
                                                                            if (tipo_dte == 34) { tabla = "factura_exenta"; }
                                                                            if (tipo_dte == 52) { tabla = "guia_despacho"; }

                                                                            string query_update_estado_acuse_recibo_dte = "UPDATE " + tabla + " SET id_acuse_recibo_cliente_fk = '" + ultimo_id + "' WHERE folio_" + tabla + " = '" + folio_dte + "'";
                                                                            conexion.Consulta(query_update_estado_acuse_recibo_dte);
                                                                        }




                                                                    }
                                                                    //UPDATEAR DATOS DEL RECEPCIONENVIO EN LA BD
                                                                    string query_update_acuse_recibo = "UPDATE acuse_recibo_cliente SET cod_acuse_recibo_cliente = '" + CodEnvio + "', fecha_acuse_recibo_cliente = '" + FchRecep + "', hora_acuse_recibo_cliente = '" + horaRecep + "', rut_emisor_acuse_recibo_cliente = '" + RutEmisor + "', estado_recep_env_acuse_recibo_cliente = '" + EstadoRecepEnv + "', recep_env_glosa_acuse_recibo_cliente = '" + RecepEnvGlosa + "', uid_correo = '"+ correo_uid + "' WHERE id_acuse_recibo_cliente = '" + ultimo_id + "'";
                                                                    conexion.Consulta(query_update_acuse_recibo);

                                                                    string mensaje2 = "[" + getFecha() + "]: DTE RECIBIDO XML ID Correo : " + correo_uid + " Acuse Recibo DTE " + tipo_dte + " Rut Emisor: " + RutEmisor + " Glosa: " + RecepEnvGlosa;
                                                                    Console.WriteLine(mensaje2);
                                                                    Consola_log = Consola_log + mensaje2 + Environment.NewLine;

                                                                }
                                                                if (nodos_resultado[m].Name == "ResultadoDTE")
                                                                {
                                                                    //ES UNA RESPUESTA DE ACEPTACION COMERCIAL DEL DTE
                                                                    List<string> list_codigo_acuse_recibo = conexion.Select("SELECT MAX(id_acuse_recibo_comercial_cliente) FROM acuse_recibo_comercial_cliente");
                                                                    int ultimo_id = int.Parse(list_codigo_acuse_recibo[0]) + 1;
                                                                    string insertar_id_acuse_recibo = "INSERT INTO acuse_recibo_comercial_cliente (id_acuse_recibo_comercial_cliente) VALUES ('" + ultimo_id + "')";
                                                                    conexion.Consulta(insertar_id_acuse_recibo);
                                                                    XmlNodeList nodos_recepcion_envio = nodos_resultado[m].ChildNodes;

                                                                    for (int n = 0; n < nodos_recepcion_envio.Count; n++)
                                                                    {
                                                                        if (nodos_recepcion_envio[n].Name == "CodEnvio")
                                                                        {
                                                                            CodEnvio = long.Parse(nodos_recepcion_envio[n].InnerText);
                                                                        }

                                                                        if (nodos_recepcion_envio[n].Name == "FchEmis")
                                                                        {
                                                                            string fecha = nodos_recepcion_envio[n].InnerText;
                                                                            //char delimitador = 'T';
                                                                            //string[] fecha_separada = fecha.Split(delimitador);
                                                                            FchRecep = fecha;
                                                                            horaRecep = "";
                                                                        }
                                                                        if (nodos_recepcion_envio[n].Name == "RUTEmisor")
                                                                        {
                                                                            RutEmisor = nodos_recepcion_envio[n].InnerText;
                                                                        }


                                                                        if (nodos_recepcion_envio[n].Name == "TipoDTE")
                                                                        {
                                                                            tipo_dte = int.Parse(nodos_recepcion_envio[n].InnerText);
                                                                        }
                                                                        if (nodos_recepcion_envio[n].Name == "Folio")
                                                                        {
                                                                            folio_dte = int.Parse(nodos_recepcion_envio[n].InnerText);
                                                                        }
                                                                        if (nodos_recepcion_envio[n].Name == "EstadoDTE")
                                                                        {
                                                                            estado_dte = int.Parse(nodos_recepcion_envio[n].InnerText);
                                                                        }
                                                                        if (nodos_recepcion_envio[n].Name == "EstadoDTEGlosa")
                                                                        {
                                                                            glosa_dte = nodos_recepcion_envio[n].InnerText;
                                                                        }






                                                                    }
                                                                    //INSERTAR DATOWS RECEPCIONDTE
                                                                    conexion.Consulta("INSERT INTO acuse_recibo_comercial_dte_cliente (id_acuse_recibo_comercial_cliente_fk,tipodte_acuse_recibo_comercial_dte_cliente,folio_acuse_recibo_comercial_dte_cliente,estado_dte_acuse_recibo_comercial_dte_cliente,glosa_dte_acuse_recibo_comercial_dte_cliente) VALUES ('" + ultimo_id + "','" + tipo_dte + "','" + folio_dte + "','" + estado_dte + "','" + glosa_dte + "')");

                                                                    //UPDATEAR EL FK EN EL DTE CORRESPONDIENTE DEPENDIENDO DEL TIPO DTE Y EL FOLIO
                                                                    string tabla = "";
                                                                    if (tipo_dte == 33) { tabla = "factura"; }
                                                                    if (tipo_dte == 61) { tabla = "nota_credito"; }
                                                                    if (tipo_dte == 56) { tabla = "nota_debito"; }
                                                                    if (tipo_dte == 34) { tabla = "factura_exenta"; }
                                                                    if (tipo_dte == 52) { tabla = "guia_despacho"; }

                                                                    string query_update_estado_acuse_recibo_dte = "UPDATE " + tabla + " SET id_acuse_recibo_comercial_cliente_fk = '" + ultimo_id + "' WHERE folio_" + tabla + " = '" + folio_dte + "'";
                                                                    conexion.Consulta(query_update_estado_acuse_recibo_dte);


                                                                    //UPDATEAR DATOS DEL RECEPCIONENVIO EN LA BD
                                                                    string query_update_acuse_recibo = "UPDATE acuse_recibo_comercial_cliente SET cod_acuse_recibo_comercial_cliente = '" + CodEnvio + "', fecha_acuse_recibo_comercial_cliente = '" + FchRecep + "', hora_acuse_recibo_comercial_cliente = '" + horaRecep + "', rut_emisor_acuse_recibo_comercial_cliente = '" + RutEmisor + "', uid_correo = '" + correo_uid + "' WHERE id_acuse_recibo_comercial_cliente = '" + ultimo_id + "'";
                                                                    conexion.Consulta(query_update_acuse_recibo);


                                                                string mensaje2 = "[" + getFecha() + "]: DTE RECIBIDO XML ID Correo : " + correo_uid + " Acuse Recibo Comercial DTE " + tipo_dte + " Rut Emisor: " + RutEmisor + " Glosa: " + glosa_dte;
                                                                    Console.WriteLine(mensaje2);
                                                                    Consola_log = Consola_log + mensaje2 + Environment.NewLine;

                                                                }
                                                            }



                                                        }
                                                    }




                                                }








                                            }
                                            else if (envio_recibos.Count != 0 && !errorAcuseRecibo)
                                            {

                                                //NO FUNCIONO EL SCHEMA DEL ENVIO RECIBO HAY QUE BUSCAR EL ORIGINAL, YA QQUE ESTE SE SACO DE LIBREDTE, LIBRERIA EN PHP


                                                //VALIDAR XML DE RESPUESTA CON SCHEMA
                                                /*
                                                string filename = filePath;
                                                string respuestaSchema = "";
                                                string schemaFileName = schemaFileName = @"C:\inetpub\wwwroot\api_agrodte\AgroDTE_Archivos\Schemas\DTEs\EnvioRecibos_v10.xsd";
                                                try
                                                {
                                                    string path_servicio_validar_xml = "";
                                                    if (es_produccion)
                                                    {
                                                        path_servicio_validar_xml = "http://192.168.1.9:90/WebServiceValidarXML/ValidarXML.asmx/ValidarXml?xmlFilename=" + filename + "&schemaFilename=" + schemaFileName;
                                                    }
                                                    else
                                                    {

                                                        path_servicio_validar_xml = "http://localhost:81/WebServiceValidarXML/ValidarXML.asmx/ValidarXml?xmlFilename=" + filename + "&schemaFilename=" + schemaFileName;
                                                    }


                                                    WebRequest request = WebRequest.Create(path_servicio_validar_xml);
                                                    request.Method = "GET";
                                                    WebResponse response = request.GetResponse();

                                                    using (var reader = new StreamReader(response.GetResponseStream()))
                                                    {
                                                        respuestaSchema = reader.ReadToEnd(); // do something fun...
                                                    }
                                                }
                                                catch (Exception e)
                                                {

                                                    File.WriteAllText(@"..\logerror.txt", e.Message);
                                                }

                                                //ESTA RESPUESTA ES UN XML PERO ES UN STRING, POR LO TANTO PARSEAMOS DE STRING A XML
                                                XmlDocument xmlDoc = new XmlDocument();
                                                xmlDoc.LoadXml(respuestaSchema);

                                                XmlNodeList elemlist = xmlDoc.GetElementsByTagName("string");
                                                string resultadoSchema = elemlist[0].InnerXml;
                                                string mensajeSchema = elemlist[1].InnerXml;*/


                                                string resultadoSchema = "True";
                                                string filename = filePath;
                                                string mensajeSchema = "";

                                                //parsear xml para obtener la respuesta de la etiqueta string "Document is valid"
                                                if (resultadoSchema == "True")
                                                {
                                                    //NO HAY ERRORES
                                                    Console.WriteLine("XML Valido");

                                                }
                                                else
                                                {
                                                    //HAY ERRORES EN XML
                                                    //CAMBIAR EL NOMBRE DEL XML Y AGREGARLE UN "ERROR" EN EL NOMBRE
                                                    string filenameError = filename.Substring(0, filename.Length - 4);
                                                    System.IO.File.Move(filename, filenameError + "ERRORSCHEMA.xml");
                                                    Console.WriteLine("XML Invalido:" + mensajeSchema);
                                                }

                                                //CORRESPONDE A UNA RESPUESTA EN XML
                                                if (conteo_adjuntos > 1)
                                                {

                                                }
                                                else
                                                {
                                                    directorio = directorio.Replace("\\", "\\\\");
                                                html_correo = html_correo.Replace("'", "\"");
                                                string insert_correo_uid = "INSERT INTO correo_intercambio (uid_correo,cuerpo_correo_texto,cuerpo_correo_html,ruta_correo,fecha_correo,casilla_correo) VALUES ('" + correo_uid + "','" + texto_correo + "','" + html_correo + "','" + directorio + "','" + correo_fecha + "','" + casilla_correo + "')";
                                                    conexion.Consulta(insert_correo_uid);
                                                }

                                                //VARIABLES PARA GUARDAR EN LA BD DE <Caratula>

                                                string RutResponde = "";
                                                string horaRecep = ""; //Este se obtiene a partir FchRecep
                                                string fechaRecep = "";

                                                //VARIABLES PARA GUARDAR EN LA BD DE <DocumentoRecibo>
                                                int tipo_dte = 0;
                                                long folio_dte = 0;
                                                string recinto = "";
                                                string rutFirma = "";
                                                string declaracion = "";


                                                for (int j = 0; j < envio_recibos.Count; j++)
                                                {
                                                    XmlNodeList nodos_envio_recibos = envio_recibos[j].ChildNodes;

                                                    for (int k = 0; k < nodos_envio_recibos.Count; k++)
                                                    {

                                                        if (nodos_envio_recibos[k].Name == "SetRecibos")
                                                        {
                                                            XmlNodeList nodos_set_recibos = nodos_envio_recibos[k].ChildNodes;

                                                            // ES UNA RESPUESTA DE ACEPTACION DE ENVIO DTE
                                                            List<string> list_codigo_acuse_recibo = conexion.Select("SELECT MAX(id_acuse_recibo_mercaderia_cliente) FROM acuse_recibo_mercaderia_cliente");
                                                            int ultimo_id = int.Parse(list_codigo_acuse_recibo[0]) + 1;
                                                            string insertar_id_acuse_recibo = "INSERT INTO acuse_recibo_mercaderia_cliente (id_acuse_recibo_mercaderia_cliente) VALUES ('" + ultimo_id + "')";
                                                            conexion.Consulta(insertar_id_acuse_recibo);

                                                            for (int m = 0; m < nodos_set_recibos.Count; m++)
                                                            {




                                                                if (nodos_set_recibos[m].Name == "Caratula")
                                                                {

                                                                    XmlNodeList nodos_caratula = nodos_set_recibos[m].ChildNodes;

                                                                    for (int n = 0; n < nodos_caratula.Count; n++)
                                                                    {
                                                                        if (nodos_caratula[n].Name == "RutResponde")
                                                                        {
                                                                            RutResponde = nodos_caratula[n].InnerText;
                                                                        }

                                                                        if (nodos_caratula[n].Name == "TmstFirmaEnv")
                                                                        {
                                                                            string fecha = nodos_caratula[n].InnerText;
                                                                            char delimitador = 'T';
                                                                            string[] fecha_separada = fecha.Split(delimitador);
                                                                            fechaRecep = fecha_separada[0];
                                                                            horaRecep = fecha_separada[1];
                                                                        }
                                                                    }


                                                                }
                                                                if (nodos_set_recibos[m].Name == "Recibo")
                                                                {
                                                                    XmlNodeList nodos_recibo = nodos_set_recibos[m].ChildNodes;

                                                                    for (int n = 0; n < nodos_recibo.Count; n++)
                                                                    {
                                                                        if (nodos_recibo[n].Name == "DocumentoRecibo")
                                                                        {
                                                                            XmlNodeList nodos_documento_recibo = nodos_recibo[n].ChildNodes;

                                                                            for (int d = 0; d < nodos_documento_recibo.Count; d++)
                                                                            {
                                                                                if (nodos_documento_recibo[d].Name == "TipoDoc")
                                                                                {
                                                                                    tipo_dte = int.Parse(nodos_documento_recibo[d].InnerText);
                                                                                }
                                                                                if (nodos_documento_recibo[d].Name == "Folio")
                                                                                {
                                                                                    folio_dte = int.Parse(nodos_documento_recibo[d].InnerText);
                                                                                }
                                                                                if (nodos_documento_recibo[d].Name == "Recinto")
                                                                                {
                                                                                    recinto = nodos_documento_recibo[d].InnerText;
                                                                                }
                                                                                if (nodos_documento_recibo[d].Name == "RutFirma")
                                                                                {
                                                                                    rutFirma = nodos_documento_recibo[d].InnerText;
                                                                                }
                                                                                if (nodos_documento_recibo[d].Name == "Declaracion")
                                                                                {
                                                                                    declaracion = nodos_documento_recibo[d].InnerText;
                                                                                }
                                                                            }

                                                                            //INSERTAR DATOS DOCUMENTO RECIBO
                                                                            conexion.Consulta("INSERT INTO acuso_recibo_dte_mercaderia_cliente (id_acuse_recibo_mercaderia_cliente_fk,tipodte_acuse_recibo_dte_mercaderia_cliente,folio_acuse_recibo_dte_mercaderia_cliente,recinto_acuse_recibo_dte_mercaderia_cliente,rutfirma_acuse_recibo_dte_mercaderia_cliente,declaracion_acuse_recibo_dte_mercaderia_cliente) VALUES ('" + ultimo_id + "','" + tipo_dte + "','" + folio_dte + "','" + recinto + "','" + rutFirma + "','" + declaracion + "')");
                                                                            //UPDATEAR EL FK EN EL DTE CORRESPONDIENTE DEPENDIENDO DEL TIPO DTE Y EL FOLIO
                                                                            string tabla = "";
                                                                            if (tipo_dte == 33) { tabla = "factura"; }
                                                                            if (tipo_dte == 61) { tabla = "nota_credito"; }
                                                                            if (tipo_dte == 56) { tabla = "nota_debito"; }
                                                                            if (tipo_dte == 34) { tabla = "factura_exenta"; }
                                                                            if (tipo_dte == 52) { tabla = "guia_despacho"; }

                                                                            string query_update_estado_acuse_recibo_dte = "UPDATE " + tabla + " SET id_acuse_recibo_mercaderia_cliente_fk = '" + ultimo_id + "' WHERE folio_" + tabla + " = '" + folio_dte + "'";
                                                                            conexion.Consulta(query_update_estado_acuse_recibo_dte);

                                                                    }
                                                                    }
                                                                }

                                                            }

                                                            string mensaje2 = "[" + getFecha() + "]: DTE RECIBIDO XML ID Correo : " + correo_uid + " Acuse Recibo Mercaderia DTE " + tipo_dte + " RutResponde: " + RutResponde;
                                                            Console.WriteLine(mensaje2);
                                                            Consola_log = Consola_log + mensaje2 + Environment.NewLine;

                                                            //UPDATEAR DATOS DEL RECEPCIONENVIO EN LA BD
                                                            string query_update_acuse_recibo = "UPDATE acuse_recibo_mercaderia_cliente SET rut_responde_acuse_recibo_mercaderia_cliente = '" + RutResponde + "', fecha_acuse_recibo_mercaderia_cliente = '" + fechaRecep + "', hora_acuse_recibo_mercaderia_cliente = '" + horaRecep + "', uid_correo = '" + correo_uid + "' WHERE id_acuse_recibo_mercaderia_cliente = '" + ultimo_id + "'";
                                                            conexion.Consulta(query_update_acuse_recibo);


                                                       

                                                    }
                                                }
                                                }



                                            }
                                            else
                                            {
                                                // NO CORRESPONDE A UN DOCUMENTO XML DTE
                                                string mensaje = "NO CORRESPONDE A UN DOCUMENTO XML DTE " + correo_uid;
                                                Console.WriteLine(mensaje);
                                                Consola_log = Consola_log + mensaje + Environment.NewLine;
                                                break;
                                            }
                                            //ARMAR POSIBLE RESPUESTA (ACUSE RECIBO)

                                            string path_api = "";
                                            if (es_produccion)
                                            {
                                                path_api = "http://192.168.1.9:90/api_agrodte/api/dte/EmitirAcuseRecibo";
                                            }
                                            else
                                            {
                                                //path_api = "https://localhost:44324/api/dte/EmitirAcuseRecibo";
                                                path_api = "http://192.168.1.9:90/api_agrodte/api/dte/EmitirAcuseRecibo";
                                            }

                                            var httpWebRequest = (HttpWebRequest)WebRequest.Create(path_api);
                                            httpWebRequest.ContentType = "application/json";
                                            httpWebRequest.Method = "POST";

                                            using (var streamWriter = new StreamWriter(httpWebRequest.GetRequestStream()))
                                            {
                                                string path = filePath.Replace("\\", "\\\\");
                                                string respuestaDteArray = "";


                                                for (int m = 0; m < respuestaDte.Count; m++)
                                                {
                                                    if (m == 0)
                                                    {
                                                        respuestaDteArray = "\"" + respuestaDte[0] + "\"";

                                                    }
                                                    else
                                                    {
                                                        respuestaDteArray = respuestaDteArray + ",\"" + respuestaDte[m] + "\"";
                                                    }


                                                }

                                                string json_str = "{" +
                                                    "\"datosAcuseRecibo\":{" +
                                                               "\"PathXML\":\"" + path + "\"," +
                                                              "\"RespuestaSobre\":\"" + respuestaSobre + "\"," +
                                                              "\"respuestaDte\":[" + respuestaDteArray + "]," +
                                                                "\"rutEmpresaResponde\":\"" + RutEmisor + "\"," +
                                                                "\"prefixPathXML\": \"" + correo_uid + "\"" +
                                                                "}" +
                                                              "}";

                                                streamWriter.Write(json_str);
                                            }

                                            var httpResponse = (HttpWebResponse)httpWebRequest.GetResponse();
                                            string result = "";
                                            using (var streamReader = new StreamReader(httpResponse.GetResponseStream()))
                                            {
                                                result = streamReader.ReadToEnd();
                                            }
                                            JObject json = JObject.Parse(result);
                                            string statusCode = json.GetValue("statusCode").ToString();

                                            if (statusCode == "500")
                                            {
                                                //ERROR
                                                string mensaje = json.GetValue("message").ToString();
                                                Console.WriteLine(mensaje);
                                                errorAcuseRecibo = true;
                                            }
                                        }
                                        else
                                        {
                                            //EXISTE YA EL REGISTRO

                                        }
                                    }
                                    else
                                    {
                                        //NO ES UN XML ADJUNTO
                                    }

                                    //Thread.Sleep(10000);
                                }


                            }
                        }
                        Console.WriteLine("[" + getFecha() + "]: DESCARGAS DE DTE INTERCAMBIOS REALIZADA CON EXITO");
                        Consola_log = Consola_log + "[" + getFecha() + "]: DESCARGAS DE DTE INTERCAMBIOS REALIZADA CON EXITO" + Environment.NewLine;

                    }
                    else
                    {
                        //NO HAY CONEXION A INTERNET
                        Console.WriteLine("[" + getFecha() + "]: NO HAY CONEXION A INTERNET PARA CHEQUER CORREO DE INTERCAMBIO");
                        Consola_log = Consola_log + "[" + getFecha() + "]: DIRECTORIO CREADO CON EXITO" + Environment.NewLine;
                    }
                }
                catch (Exception e)
                {
                    string mensaje = "ERROR AL CARGAR XML DE RESPUESTA SCHEMA EN FUNCION descagarFacturaCompra() : " + e.Message;
                    Console.WriteLine(mensaje);
                    Consola_log = Consola_log + mensaje + Environment.NewLine;
                   

                }
               

            


        }
       

        //VERIFICA DIRECTORIO SI ESTA CREADO O NO DE CONSUMO FOLIOS
        public static void verificarDirectorio(string directorio)
        {
            if (!Directory.Exists(directorio))
            {
                Console.WriteLine("[" + getFecha() + "]: NO EXISTE DIRECTORIO RAIZ " + directorio + ", INTENTANDO CREAR DIRECTORIO...");
                Consola_log = Consola_log + "[" + getFecha() + "]: NO EXISTE DIRECTORIO RAIZ " + directorio + ", INTENTANDO CREAR DIRECTORIO..." + Environment.NewLine;
                try
                {
                    Directory.CreateDirectory(directorio);
                    Console.WriteLine("[" + getFecha() + "]: DIRECTORIO CREADO CON EXITO");
                    Consola_log = Consola_log + "[" + getFecha() + "]: DIRECTORIO CREADO CON EXITO" + Environment.NewLine;
                }
                catch (Exception e)
                {
                    Console.WriteLine("[" + getFecha() + "]: ERROR AL CREAR DIRECTORIO: " + e.Message);
                    Consola_log = Consola_log + "[" + getFecha() + "]: ERROR AL CREAR DIRECTORIO: " + e.Message + Environment.NewLine;


                }

            }
        }

        //ENVIA CONSUMO DE FOLIOS DE BOLETAS
        public static void EnviarConsumoFolios()
        {
            try
            {
                ConexionBD conexion = new ConexionBD();

                //RESCATAMOS LA FECHA DEL DIA ANTERIOR
                DateTime dateForButton = DateTime.Now.AddDays(-1);
                string fecha_anterior = dateForButton.ToString("yyyy-MM-dd");


                //VERIFICAR SI YA SE ENVIO UN CONSUMO DE FOLIOS
                string query_verificar = "SELECT id_envio_dte_fk FROM consumo_folios WHERE fecha_inicio = '" + fecha_anterior + "'";
                List<string> resultados_query_verificar = conexion.Select(query_verificar);

                if (resultados_query_verificar.Any())
                {
                    //EXISTE CONSUMO DE FOLIOS ENVIADO

                    Console.WriteLine("[" + getFecha() + "]: EXISTE CONSUMO DE FOLIOS ENVIADO, TRACK_ID:  " + resultados_query_verificar[0]);
                    Consola_log = Consola_log + "[" + getFecha() + "]: EXISTE CONSUMO DE FOLIOS ENVIADO, TRACK_ID:  " + resultados_query_verificar[0] + Environment.NewLine;
                }
                else
                {
                    //NO EXISTE CONSUMO ENVIADO
                    //RESCATAR EL ULTIMO FOLIO DE UN CONSUMO
                    string query_ultimo_folio = "SELECT MAX(id_consumo_folios) FROM consumo_folios";
                    List<string> resultados_ultimo_folio = conexion.Select(query_ultimo_folio);

                    int folio_nuevo = int.Parse(resultados_ultimo_folio[0]) + 1;



                    //RESCATAR DATOS DE DTE 39 Y 41

                    //----------------------------------------------BOLETA 39 -----------------------------------------------------//
                    string query_suma_mnttotal_utilizados_39 = "SELECT SUM(mnttotal_boleta) AS suma_mnttotal, COUNT(id_envio_dte_fk) AS folio_emitidos FROM boleta WHERE fechaemis_boleta = '" + fecha_anterior + "'";
                    List<string> resultados_suma_utilizados_39 = conexion.Select(query_suma_mnttotal_utilizados_39);

                    string suma_montototal_39 = "";
                    if (resultados_suma_utilizados_39[0] == "")
                    {
                        suma_montototal_39 = "0";
                    }
                    else
                    {
                        suma_montototal_39 = resultados_suma_utilizados_39[0];
                    }

                    string folios_utilizados_39 = resultados_suma_utilizados_39[1];


                    string query_folio_boleta = "SELECT MIN(folio_boleta) as primer_folio, MAX(folio_boleta) as ultimo_folio FROM boleta WHERE fechaemis_boleta = '" + fecha_anterior + "'";
                    List<string> resultados_folio_boleta = conexion.Select(query_folio_boleta);

                    int primer_folio_boleta = 0;
                    int ultimo_folio_boleta = 0;

                    if (resultados_folio_boleta[0] == "")
                    {
                        primer_folio_boleta = 0;
                        ultimo_folio_boleta = 0;
                    }
                    else
                    {
                        primer_folio_boleta = int.Parse(resultados_folio_boleta[0]);
                        ultimo_folio_boleta = int.Parse(resultados_folio_boleta[1]);
                    }


                    int folios_anulados_39 = 0;

                    for (int i = primer_folio_boleta; i <= ultimo_folio_boleta; i++)
                    {
                        //COMPARAR EL i CON EL CORRELATIVO
                        string query_comparar = "SELECT folio_boleta FROM boleta WHERE folio_boleta = '" + i + "'";
                        List<string> resultados_comparar = conexion.Select(query_comparar);

                        if (resultados_comparar.Count == 0)
                        { // NO EXISTE BOLETA, FOLIO QUE NO SE UTILIZO
                            folios_anulados_39++;
                        }
                        else
                        {//EXISTE BOLETA

                        }
                    }

                    /*"SELECT folio_boleta FROM boleta WHERE fechaemis_boleta = '" +fecha_anterior+"'"
                    string query_folios_anulados_39 = "SELECT COUNT(folio_nota_credito) FROM nota_credito INNER JOIN boleta ON nota_credito.folioref_nota_credito = boleta.folio_boleta WHERE nota_credito.fchemis_nota_credito = '" + fecha_anterior + "'";
                    List<string> resultados_folios_anulados_39 = conexion.Select(query_folios_anulados_39);
                    string folios_anulados_39 = resultados_folios_anulados_39[0];*/
                    
                    if (int.Parse(folios_utilizados_39) == 0)
                    {
                        folios_anulados_39 = 0;
                    }
                    

                   int folios_emitidos_39 = int.Parse(folios_utilizados_39) - folios_anulados_39;


                    //----------------------------------------------BOLETA 41 -----------------------------------------------------//

                    string query_suma_mnttotal_utilizados_41 = "SELECT SUM(mnttotal_boleta_exenta) AS suma_mnttotal, COUNT(id_envio_dte_fk) AS folio_emitidos FROM boleta_exenta WHERE fechaemis_boleta_exenta = '" + fecha_anterior + "'";
                    List<string> resultados_suma_utilizados_41 = conexion.Select(query_suma_mnttotal_utilizados_41);

                    string suma_montototal_41 = "";
                    if (resultados_suma_utilizados_41[0] == "")
                    {
                        suma_montototal_41 = "0";
                    }
                    else
                    {
                        suma_montototal_41 = resultados_suma_utilizados_41[0];
                    }

                    string folios_utilizados_41 = resultados_suma_utilizados_41[1];

                    string query_folio_boleta_exenta = "SELECT MIN(folio_boleta_exenta) as primer_folio, MAX(folio_boleta_exenta) as ultimo_folio FROM boleta_exenta WHERE fechaemis_boleta_exenta = '" + fecha_anterior + "'";
                    List<string> resultados_folio_boleta_exenta = conexion.Select(query_folio_boleta_exenta);

                    int primer_folio_boleta_exenta = 0;
                    int ultimo_folio_boleta_exenta = 0;

                    if (resultados_folio_boleta_exenta[0] == "")
                    {
                        primer_folio_boleta_exenta = 0;
                        ultimo_folio_boleta_exenta = 0;
                    }
                    else
                    {
                        primer_folio_boleta_exenta = int.Parse(resultados_folio_boleta_exenta[0]);
                        ultimo_folio_boleta_exenta = int.Parse(resultados_folio_boleta_exenta[1]);
                    }

                    int folios_anulados_41 = 0;

                    for (int i = primer_folio_boleta_exenta; i <= ultimo_folio_boleta_exenta; i++)
                    {
                        //COMPARAR EL i CON EL CORRELATIVO
                        string query_comparar = "SELECT folio_boleta_exenta FROM boleta_exenta WHERE folio_boleta_exenta = '" + i + "'";
                        List<string> resultados_comparar = conexion.Select(query_comparar);

                        if (!resultados_comparar.Any())
                        { // NO EXISTE BOLETA, FOLIO QUE NO SE UTILIZO
                            if (i == 0)
                            {

                            }
                            else
                            {
                                folios_anulados_41++;
                            }

                        }
                        else
                        {//EXISTE BOLETA

                        }
                    }

                    /* string query_folios_anulados_41 = "SELECT COUNT(folio_nota_credito) FROM nota_credito INNER JOIN boleta_exenta ON nota_credito.folioref_nota_credito = boleta_exenta.folio_boleta_exenta WHERE nota_credito.fchemis_nota_credito = '" + fecha_anterior + "'";
                     List<string> resultados_folios_anulados_41 = conexion.Select(query_folios_anulados_41);
                     string folios_anulados_41 = resultados_folios_anulados_41[0];*/

                    int folios_emitidos_41 = int.Parse(folios_utilizados_41) - folios_anulados_41;



                    string fecha_actual = DateTime.Now.ToString("yyyy-MM-dd");
                    //string fecha_actual = "2022-02-20";
                    string fechahhmm = System.DateTime.Now.ToString("yyyy-MM-ddTHH:mm:ss");
                    string fecha_archivo = System.DateTime.Now.ToString("yyyyMMddHHmmss");

                    //DIRECTORIO DEL XML

                    string NombreArchivo = "ConsumoFolios" + fecha_archivo;
                    string directorioFechaActual = crearDirectorio(NombreArchivo);
                    string fileNameXML = directorioFechaActual + @"\" + NombreArchivo + ".xml";

                    string directorioDTE = Path.Combine(directorio_archivos, "", fileNameXML);

                    //DIRECTORIO SOBRE ENVIO XML

                    string fileNameXMLSobre = directorioFechaActual + @"\SobreEnvioConsumoFolios" + folio_nuevo + ".xml";
                    string EnvioDTE_xml = Path.Combine(directorio_archivos, "", fileNameXMLSobre);

                    //CREAR XML CONSUMO FOLIOS
                    XmlTextWriter writer;
                    writer = new XmlTextWriter(directorioDTE, Encoding.GetEncoding("ISO-8859-1"));
                    writer.Formatting = System.Xml.Formatting.Indented;
                    writer.Indentation = 0;

                    writer.WriteStartDocument();

                    writer.WriteStartElement("ConsumoFolios");
                    writer.WriteAttributeString("xmlns", "http://www.sii.cl/SiiDte");
                    writer.WriteAttributeString("xmlns:xsi", "http://www.w3.org/2001/XMLSchema-instance");
                    writer.WriteAttributeString("xsi:schemaLocation", "http://www.sii.cl/SiiDte ConsumoFolio_v10.xsd");
                    writer.WriteAttributeString("version", "1.0");


                    writer.WriteStartElement("DocumentoConsumoFolios");
                    writer.WriteAttributeString("ID", "Consumo_Folios_" + folio_nuevo.ToString());

                    writer.WriteStartElement("Caratula");
                    writer.WriteAttributeString("version", "1.0");
                    writer.WriteElementString("RutEmisor", RutEmisor);
                    writer.WriteElementString("RutEnvia", RutEnvia);
                    writer.WriteElementString("FchResol", FchResol);
                    writer.WriteElementString("NroResol", NroResol);
                    writer.WriteElementString("FchInicio", fecha_anterior);
                    writer.WriteElementString("FchFinal", fecha_anterior);
                    writer.WriteElementString("SecEnvio", "1"); //SI SE DESEA ENVIAR OTRO CONSUMO DE FOLIOS PARA RECTIFICAR SE DEBE AUMENTAR ESTE VALOR
                    writer.WriteElementString("TmstFirmaEnv", fechahhmm);
                    writer.WriteEndElement();

                    writer.WriteStartElement("Resumen");
                    writer.WriteElementString("TipoDocumento", "39");
                    writer.WriteElementString("MntTotal", suma_montototal_39);
                    writer.WriteElementString("FoliosEmitidos", folios_emitidos_39.ToString());
                    writer.WriteElementString("FoliosAnulados", folios_anulados_39.ToString());
                    writer.WriteElementString("FoliosUtilizados", folios_utilizados_39);
                    writer.WriteEndElement();

                    writer.WriteStartElement("Resumen");
                    writer.WriteElementString("TipoDocumento", "41");
                    writer.WriteElementString("MntExento", suma_montototal_41);
                    writer.WriteElementString("MntTotal", suma_montototal_41);
                    writer.WriteElementString("FoliosEmitidos", folios_emitidos_41.ToString());
                    writer.WriteElementString("FoliosAnulados", folios_anulados_41.ToString());
                    writer.WriteElementString("FoliosUtilizados", folios_utilizados_41);
                    writer.WriteEndElement();

                    writer.WriteEndElement();
                    writer.WriteEndElement();
                    writer.WriteEndDocument();
                    writer.Flush();
                    writer.Close();

                    //GUARDAR XML
                    XmlDocument xml_Dte = new XmlDocument();
                    xml_Dte.PreserveWhitespace = true;
                    xml_Dte.Load(directorioDTE);
                    xml_Dte.InnerXml = xml_Dte.InnerXml.Replace("<?xml version=\"1.0\" encoding=\"iso-8859-1\"?>", "<?xml version=\"1.0\" encoding=\"ISO-8859-1\"?>");
                    xml_Dte.Save(EnvioDTE_xml);

                    //FIRMAR XML
                    StreamReader xml_firmar = new StreamReader(EnvioDTE_xml, Encoding.GetEncoding("ISO-8859-1"));
                    string xml_firmar_setdte = xml_firmar.ReadToEnd();
                    xml_firmar.Close();

                    string nombrecertificado = "RUBEN SALATIEL RIVERA GALLEGUILLOS";
                    Certificado certificado = new Certificado();
                    string firmado = certificado.Firmar(nombrecertificado, xml_firmar_setdte, "#Consumo_Folios_" + folio_nuevo.ToString());
                    XDocument xml_firmado = XDocument.Parse(firmado, LoadOptions.PreserveWhitespace);
                    xml_firmado.Save(EnvioDTE_xml);

                    XmlDocument oDoc3 = new XmlDocument();
                    oDoc3.PreserveWhitespace = true;
                    oDoc3.Load(EnvioDTE_xml);
                    oDoc3.InnerXml = oDoc3.InnerXml.Replace("<?xml version=\"1.0\" encoding=\"iso-8859-1\"?>", "<?xml version=\"1.0\" encoding=\"ISO-8859-1\"?>");
                    oDoc3.Save(EnvioDTE_xml);

                    //GUARDAR EN LA BD EL CONSUMO DE FOLIOS


                    //INSERTAR DATOS
                    //INSERTAR DATOS DE BOLETAS
                    string query_folios_consumidos_boleta = "INSERT INTO consumo_folios (id_consumo_folios,fecha_inicio, fecha_final, tipo_docu, mnt_total, folios_emitidos, folios_anulados, folios_utilizados,secuencia_envio) VALUES (" + folio_nuevo + ",'" + fecha_anterior + "', '" + fecha_anterior + "', '39', '" + suma_montototal_39 + "', '" + folios_emitidos_39 + "', '" + folios_anulados_39 + "', '" + folios_utilizados_39 + "','1')";
                    conexion.Consulta(query_folios_consumidos_boleta);

                    //INSERTAR DATOS DE BOLETAS EXENTAS

                    int folio_registro_boletas_exentas = folio_nuevo + 1;
                    string query_folios_consumidos_boleta_exenta = "INSERT INTO consumo_folios (id_consumo_folios,fecha_inicio, fecha_final, tipo_docu, mnt_total, folios_emitidos, folios_anulados, folios_utilizados,secuencia_envio) VALUES (" + folio_registro_boletas_exentas + ",'" + fecha_anterior + "', '" + fecha_anterior + "', '41', '" + suma_montototal_41 + "', '" + folios_emitidos_41 + "', '" + folios_anulados_41 + "', '" + folios_utilizados_41 + "','1')";
                    conexion.Consulta(query_folios_consumidos_boleta_exenta);

                    //GUARDAR EN LA BASE DE DATOS EL SOBRE DE ENVIO SIN NUMERO DE TRACK ID AUN
                    string queryUpdateDirectorio = "INSERT INTO envio_dte (estado_envio_dte,rutaxml_envio_dte,fecha_envio_dte) VALUES ('No Enviado','" + EnvioDTE_xml + "',NOW())";
                    queryUpdateDirectorio = queryUpdateDirectorio.Replace("\\", "\\\\");
                    conexion.Consulta(queryUpdateDirectorio);

                    //RESCATAR E INSERTAR EL ID DEL SOBRE EN LA TABLA CONSUMO FOLIO
                    string queryIdSobre = "SELECT id_envio_dte FROM envio_dte WHERE rutaxml_envio_dte = '" + EnvioDTE_xml + "'";
                    queryIdSobre = queryIdSobre.Replace("\\", "\\\\");
                    List<string> ResultIdSobre = conexion.Select(queryIdSobre);

                    //UPDATEAR REGISTRO DE BOLETAS
                    string queryUpdateidSobre_Boletas = "UPDATE consumo_folios SET id_envio_dte_fk = '" + ResultIdSobre[0] + "' WHERE id_consumo_folios = '" + folio_nuevo + "'";
                    conexion.Consulta(queryUpdateidSobre_Boletas);

                    //UPDATEAR EL REGISTRO DE BOLETAS EXENTAS


                    string queryUpdateidSobre_Boletas_Exentas = "UPDATE consumo_folios SET id_envio_dte_fk = '" + ResultIdSobre[0] + "' WHERE id_consumo_folios = '" + folio_registro_boletas_exentas + "'";
                    conexion.Consulta(queryUpdateidSobre_Boletas_Exentas);


                    //CHEQUEAR SI HAY CONEXION A INTERNET 
                    string respuestaPing = checkPing("" + servidor_facturas + ".sii.cl");

                    if (respuestaPing == "Conexion Exitosa")
                    {
                        //ENVIAR SOBRE
                        //EXISTE CONEXION, SOLICITAR SEMILLA, TOKEN Y ENVIAR SOBRE DTE
                        string respuestaEnvio = "";
                        string TrackId_str = "";


                        respuestaEnvio = enviarSobre(EnvioDTE_xml, RutEnvia, RutEmisor);
                        //string respuestaEnvio es el TRACKID EN XML
                        XmlDocument xmlDoc2 = new XmlDocument();
                        xmlDoc2.LoadXml(respuestaEnvio);
                        XmlNodeList TrackId = xmlDoc2.GetElementsByTagName("string");
                        TrackId_str = TrackId[0].InnerXml;

                        if (int.TryParse(TrackId_str, out int numeroEnvio))
                        {
                            //GUARDAR ESTADO DE ENVIO CON TRACKID
                            string queryUpdateTrackid = "UPDATE envio_dte SET trackid_envio_dte = '" + TrackId_str + "', estado_envio_dte = 'Enviado' WHERE rutaxml_envio_dte = '" + EnvioDTE_xml + "';";
                            queryUpdateTrackid = queryUpdateTrackid.Replace("\\", "\\\\");
                            conexion.Consulta(queryUpdateTrackid);

                            Console.WriteLine("[" + getFecha() + "]: CONSUMO DE FOLIOS ENVIADO EXITOSAMENTE " + TrackId_str);
                            Consola_log = Consola_log + "[" + getFecha() + "]: CONSUMO DE FOLIOS ENVIADO EXITOSAMENTE " + TrackId_str + Environment.NewLine;

                        }
                        else
                        {
                            Console.WriteLine("[" + getFecha() + "]: Error de Track id: " + TrackId_str);
                            Consola_log = Consola_log + "[" + getFecha() + "]: Error de Track id: " + TrackId_str + Environment.NewLine;
                        }

                    }
                    else
                    {
                        Console.WriteLine("[" + getFecha() + "]: No hay conexion a internet o con servidor: " + servidor_facturas);
                        Consola_log = Consola_log + "[" + getFecha() + "]: No hay conexion a internet o con servidor: " + servidor_facturas + Environment.NewLine;
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("[" + getFecha() + "]: ERROR EN FUNCION EnviarConsumoFolios() : " + ex.Message);
                Consola_log = Consola_log + "[" + getFecha() + "]: ERROR EN FUNCION EnviarConsumoFolios() : " + ex.Message + Environment.NewLine;

            }
            


            

        }

        //VERIFICA SI EL TOKEN SE HA UTILIZADO DURANTE LOS ULTIMOS 30MIN, SI PASA ESTE TIEMPO LO INVALIDA PARA QUE LA API PIDA OTRO TOKEN
        private static void VerificarVigenciaToken(string servidor)
        {
            try
            {
                string query_token_boleta = "SELECT fecha_ultimo_uso_token,id_token,digitos_token FROM token WHERE estado_token = '1' AND servidor_token = '" + servidor + "'";
                ConexionBD conexion = new ConexionBD();
                List<string> fecha_ultimo_uso_token = conexion.SelectFecha(query_token_boleta);
                //fecha_ultimo_uso_token[0] = fecha ultimo uso token
                //fecha_ultimo_uso_token[1] = id_token
                //fecha_ultimo_uso_token[2] = digitos_token


                if (fecha_ultimo_uso_token.Count == 0)
                {
                    //NO HAY TOKEN ACTIVO EN LA BD
                    Console.ForegroundColor = ConsoleColor.Red;

                    //DEPENDE DEL SERVIDOR LANZA MENSAJE QUE TOKEN ESTA INACTIVO
                    if (servidor == "api" || servidor == "apicert")
                    {
                        Console.WriteLine("[" + getFecha() + "]: NO EXISTE TOKEN DE BOLETAS ACTIVO EN LA BASE DE DATOS");
                        Consola_log = Consola_log + "[" + getFecha() + "]: NO EXISTE TOKEN DE BOLETAS ACTIVO EN LA BASE DE DATOS" + Environment.NewLine;
                    }
                    if (servidor == "maullin" || servidor == "palena")
                    {
                        Console.WriteLine("[" + getFecha() + "]: NO EXISTE TOKEN DE FACTURAS ACTIVO EN LA BASE DE DATOS");
                        Consola_log = Consola_log + "[" + getFecha() + "]: NO EXISTE TOKEN DE FACTURAS ACTIVO EN LA BASE DE DATOS" + Environment.NewLine;
                    }
                    if (servidor == "zeusr.sii.cl")
                    {
                        Console.WriteLine("[" + getFecha() + "]: NO EXISTE TOKEN DE PORTAL SII ACTIVO EN LA BASE DE DATOS");
                        Consola_log = Consola_log + "[" + getFecha() + "]: NO EXISTE TOKEN DE PORTAL SII ACTIVO EN LA BASE DE DATOS" + Environment.NewLine;
                    }

                    Console.ForegroundColor = ConsoleColor.White;
                }
                else
                {
                    //obtenemos fecha y hora actual
                    //string fecha_hora_actual = DateTime.Now.ToString("yyyy/MM/dd HH:mm:ss");

                    DateTime ultimo_uso_token = DateTime.Parse(fecha_ultimo_uso_token[0]);

                    DateTime fecha_actual = DateTime.ParseExact(datetime_str, "yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);

                    TimeSpan diferencia_fechas = fecha_actual.Subtract(ultimo_uso_token);

                    TimeSpan mediahora = TimeSpan.FromMinutes(30);

                    //SI LA DIFERENCIA ES MAYOR A 30MIN 

                    if (diferencia_fechas > mediahora)
                    {
                        //TOKEN FUERA DE TIEMPO, ACTUALIZAR ESTADO A 0 EN BD
                        Console.ForegroundColor = ConsoleColor.Red;
                        if (servidor == "api" || servidor == "apicert")
                        {
                            Console.WriteLine("[" + getFecha() + "]: TOKEN DE BOLETAS FUERA DE TIEMPO, DIFERENCIA: " + diferencia_fechas);
                            Consola_log = Consola_log + "[" + getFecha() + "]: TOKEN DE BOLETAS FUERA DE TIEMPO, DIFERENCIA: " + diferencia_fechas + Environment.NewLine;
                        }

                        if (servidor == "maullin" || servidor == "palena")
                        {
                            Console.WriteLine("[" + getFecha() + "]: TOKEN DE FACTURAS FUERA DE TIEMPO, DIFERENCIA: " + diferencia_fechas);
                            Consola_log = Consola_log + "[" + getFecha() + "]: TOKEN DE FACTURAS FUERA DE TIEMPO, DIFERENCIA: " + diferencia_fechas + Environment.NewLine;
                        }
                        if (servidor == "zeusr.sii.cl")
                        {
                            Console.WriteLine("[" + getFecha() + "]: TOKEN DE PORTAL SII FUERA DE TIEMPO, DIFERENCIA: " + diferencia_fechas);
                            Consola_log = Consola_log + "[" + getFecha() + "]: TOKEN DE PORTAL SII FUERA DE TIEMPO, DIFERENCIA: " + diferencia_fechas + Environment.NewLine;
                        }
                        Console.ForegroundColor = ConsoleColor.White;

                        conexion.Consulta("UPDATE token SET estado_token='0' WHERE id_token='" + fecha_ultimo_uso_token[1] + "' AND servidor_token='" + servidor + "'");


                    }
                    else
                    {

                        Console.ForegroundColor = ConsoleColor.White;
                        if (servidor == "api" || servidor == "apicert")
                        {
                            Console.WriteLine("[" + getFecha() + "]: TOKEN VALIDO DE BOLETAS AUN, ULTIMO USO: " + ultimo_uso_token);
                            Consola_log = Consola_log + "[" + getFecha() + "]: TOKEN VALIDO DE BOLETAS AUN, ULTIMO USO: " + ultimo_uso_token + Environment.NewLine;
                        }

                        if (servidor == "maullin" || servidor == "palena")
                        {
                            Console.WriteLine("[" + getFecha() + "]: TOKEN VALIDO DE FACTURAS AUN, ULTIMO USO: " + ultimo_uso_token);
                            Consola_log = Consola_log + "[" + getFecha() + "]: TOKEN VALIDO DE FACTURAS AUN, ULTIMO USO: " + ultimo_uso_token + Environment.NewLine;
                        }
                        if (servidor == "zeusr.sii.cl")
                        {
                            Console.WriteLine("[" + getFecha() + "]: TOKEN VALIDO DE PORTAL SII AUN, ULTIMO USO: " + ultimo_uso_token);
                            Consola_log = Consola_log + "[" + getFecha() + "]: TOKEN VALIDO DE PORTAL SII AUN, ULTIMO USO: " + ultimo_uso_token + Environment.NewLine;
                        }

                        Console.ForegroundColor = ConsoleColor.White;


                    }


                }
            }
            catch (Exception ex)
            {

                Console.WriteLine("[" + getFecha() + "]: ERROR EN FUNCION VerificarVigenciaToken() : " + ex.Message);
                Consola_log = Consola_log + "[" + getFecha() + "]: ERROR EN FUNCION VerificarVigenciaToken() : "+ ex.Message + Environment.NewLine;
            }

           
          




        }

        //RESCATA LA RESPUESTA POR PARTE DEL SII SI FUERON ACEPTADOS, RECHAZADOS O EN REPARO LOS SOBRES.
        public static void validarSobresEnvio(TimeSpan horaActual, string servidor)
        {
            try
            {
                string datetime_str = DateTime.Now.ToString("dd/MM/yyyy HH:mm:ss");
                string diaActual = DateTime.Now.ToString("yyyy-MM-dd");
                // SACAR DIAS ANTERIORES
                string diaAnterior = DateTime.Now.AddDays(-1).ToString("yyyy-MM-dd");

                //SE DEBE RESTAR UNOS 5 MINUTOS A LA HORA ACTUAL PORQUE EL SII DEMORA EN VALIDAR LOS SOBRES.
                TimeSpan horaUpdateada = horaActual.Add(new TimeSpan(0, -5, 0));

                //TRAER LOS NUMEROS DE SOBRE DEPENDIENDO DE QUE TIPO DE DOCUMENTO ES
                List<string> listaTracksId = new List<string>();
                ConexionBD conexion = new ConexionBD();
                if (servidor == "api" || servidor == "apicert")
                {
                    string query_boleta_exenta = "SELECT trackid_envio_dte FROM envio_dte INNER JOIN boleta_exenta ON boleta_exenta.id_envio_dte_fk = envio_dte.id_envio_dte WHERE trackid_envio_dte != '0' AND revision_envio_dte = '0' AND fecha_envio_dte >= '" + diaAnterior + " 08:00:00' AND fecha_envio_dte <= '" + diaActual + " " + horaUpdateada.Hours + ":" + horaUpdateada.Minutes + ":00'";
                    string query_boleta = "SELECT trackid_envio_dte FROM envio_dte INNER JOIN boleta ON boleta.id_envio_dte_fk = envio_dte.id_envio_dte WHERE trackid_envio_dte != '0' AND revision_envio_dte = '0' AND fecha_envio_dte >= '" + diaAnterior + " 08:00:00' AND fecha_envio_dte <= '" + diaActual + " " + horaUpdateada.Hours + ":" + horaUpdateada.Minutes + ":00'";
                    List<string> listaTracksId_boletas = conexion.Select(query_boleta);
                    List<string> listaTracksId_boletas_exentas = conexion.Select(query_boleta_exenta);

                    listaTracksId = listaTracksId_boletas.Concat(listaTracksId_boletas_exentas).ToList();

                }
                if (servidor == "maullin" || servidor == "palena")
                {
                    string query_factura = "SELECT trackid_envio_dte FROM envio_dte INNER JOIN factura ON factura.id_envio_dte_fk = envio_dte.id_envio_dte WHERE trackid_envio_dte != '0' AND revision_envio_dte = '0' AND fecha_envio_dte >= '" + diaAnterior + " 08:00:00' AND fecha_envio_dte <= '" + diaActual + " " + horaUpdateada.Hours + ":" + horaUpdateada.Minutes + ":00'";
                    string query_factura_exenta = "SELECT trackid_envio_dte FROM envio_dte INNER JOIN factura_exenta ON factura_exenta.id_envio_dte_fk = envio_dte.id_envio_dte WHERE trackid_envio_dte != '0' AND revision_envio_dte = '0' AND fecha_envio_dte >= '" + diaAnterior + " 08:00:00' AND fecha_envio_dte <= '" + diaActual + " " + horaUpdateada.Hours + ":" + horaUpdateada.Minutes + ":00'";
                    string query_notacredito = "SELECT trackid_envio_dte FROM envio_dte INNER JOIN nota_credito ON nota_credito.id_envio_dte_fk = envio_dte.id_envio_dte WHERE trackid_envio_dte != '0' AND revision_envio_dte = '0' AND fecha_envio_dte >= '" + diaAnterior + " 08:00:00' AND fecha_envio_dte <= '" + diaActual + " " + horaUpdateada.Hours + ":" + horaUpdateada.Minutes + ":00'";
                    string query_notadebito = "SELECT trackid_envio_dte FROM envio_dte INNER JOIN nota_debito ON nota_debito.id_envio_dte_fk = envio_dte.id_envio_dte WHERE trackid_envio_dte != '0' AND revision_envio_dte = '0' AND fecha_envio_dte >= '" + diaAnterior + " 08:00:00' AND fecha_envio_dte <= '" + diaActual + " " + horaUpdateada.Hours + ":" + horaUpdateada.Minutes + ":00'";
                    string query_guiadespacho = "SELECT trackid_envio_dte FROM envio_dte INNER JOIN guia_despacho ON guia_despacho.id_envio_dte_fk = envio_dte.id_envio_dte WHERE trackid_envio_dte != '0' AND revision_envio_dte = '0' AND fecha_envio_dte >= '" + diaAnterior + " 08:00:00' AND fecha_envio_dte <= '" + diaActual + " " + horaUpdateada.Hours + ":" + horaUpdateada.Minutes + ":00'";
                    string query_consumofolio = "SELECT trackid_envio_dte FROM envio_dte INNER JOIN consumo_folios ON consumo_folios.id_envio_dte_fk = envio_dte.id_envio_dte WHERE trackid_envio_dte != '0' AND revision_envio_dte = '0' AND fecha_envio_dte >= '" + diaAnterior + " 08:00:00' AND fecha_envio_dte <= '" + diaActual + " " + horaUpdateada.Hours + ":" + horaUpdateada.Minutes + ":00'";
                    List<string> listaTracksId_facturas = conexion.Select(query_factura);
                    List<string> listaTracksId_facturas_exentas = conexion.Select(query_factura_exenta);
                    List<string> listaTracksId_notacredito = conexion.Select(query_notacredito);
                    List<string> listaTracksId_notadebito = conexion.Select(query_notadebito);
                    List<string> listaTracksId_guiadespacho = conexion.Select(query_guiadespacho);
                    List<string> listaTracksId_consumofolio = conexion.Select(query_consumofolio);

                    listaTracksId = listaTracksId_facturas.Concat(listaTracksId_facturas_exentas).Concat(listaTracksId_notacredito).Concat(listaTracksId_notadebito).Concat(listaTracksId_guiadespacho).Concat(listaTracksId_consumofolio).ToList();

                }
                //string query = "SELECT trackid_envio_dte FROM envio_dte WHERE trackid_envio_dte != '0' AND revision_envio_dte = '0' AND fecha_envio_dte >= '" + diaActual+" 08:00:00' AND fecha_envio_dte <= '"+diaActual+" "+horaUpdateada.Hours+":"+horaUpdateada.Minutes+":00'";







                Console.WriteLine("[" + getFecha() + "]: Chequeando Sobres...");
                Consola_log = Consola_log + "[" + getFecha() + "]: Chequeando Sobres..." + Environment.NewLine;

                //CHEQUEAR SI HAY CONEXION A INTERNET 

                string respuestaPing = checkPing(servidor + ".sii.cl");


                if (respuestaPing == "Conexion Exitosa")
                {
                    if (listaTracksId.Count == 0)
                    {

                        Console.WriteLine("[" + getFecha() + "]: No hay sobres para chequear estado");
                        Consola_log = Consola_log + "[" + getFecha() + "]: No hay sobres para chequear estado" + Environment.NewLine;
                    }
                    else
                    {
                        for (int i = 0; i < listaTracksId.Count; i++)
                        {
                            updateEstadoSobre(listaTracksId[i], servidor);
                            Thread.Sleep(2000); //ESTO SE COLOCA PENSANDO QUE PUEDE SOLUCIONAR LA RESPUESTA DESDE SII CON EL CODIGO -11 EN ALGUNAS FACTURAS. YA QUE PUEDE SER UNA SOBRECARCA DEL MICROSERVICIO.
                        }

                    }

                }
                else
                {
                    Console.WriteLine("No hay conexion a internet o con servidor: " + servidor);
                    Consola_log = Consola_log + "No hay conexion a internet o con servidor: " + servidor + Environment.NewLine;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("[" + getFecha() + "]: HUBO ERROR EN ValidarSobreEnvio(): "+ex.Message);
                Consola_log = Consola_log + "[" + getFecha() + "]: HUBO ERROR EN ValidarSobreEnvio(): " + ex.Message + Environment.NewLine;

            }
            

        }

        //CONSULTA EL ESTADO DEL SOBRE Y UPDATEA A ENVIADO O NO ENVIADO Y SI FUE CON REPARO, RECHAZADO O ACEPTADO INVOCADA POR validarSobresEnvio()
        public static void updateEstadoSobre(string TrackId_str, string servidor)
        {
            if (TrackId_str != "")
            {


                try
                {
                    string datetime_str = DateTime.Now.ToString("dd/MM/yyyy HH:mm:ss");

                    ConexionBD conexion = new ConexionBD();
                    //CONSULTAR ESTADO DE ENVIO

                    string resultadoEnvio = "";
                    string estado_str = "";
                    string detalle_str = "";
                    XmlDocument xmlDoc = new XmlDocument();
                    dynamic respuesta_envio_estado_json;

                    if (servidor == "api" || servidor == "apicert")
                    {
                        //OBTENER TOKEN PARA CONSULTAR ESTADO DE ENVIO
                        string token = "";

                        //VERIFICAR SI EXISTE UN TOKEN ACTIVO EN LA BASE DE DATOS
                        List<string> respuesta_token_activo = conexion.SelectString("SELECT digitos_token FROM token WHERE estado_token = 1 AND servidor_token = '" + servidor + "'");
                        if (respuesta_token_activo.Count == 0)
                        {
                            //NO EXISTE TOKEN ACTIVO DE BOLETAS
                            Console.WriteLine("[" + getFecha() + "]: NO EXISTE TOKEN ACTIVO DE BOLETAS");
                            Consola_log = Consola_log + "[" + getFecha() + "]: NO EXISTE TOKEN ACTIVO DE BOLETAS" + Environment.NewLine;
                            return;
                        }
                        else
                        {
                            //EXISTE TOKEN VALIDO DE BOLETAS
                            token = respuesta_token_activo[0];
                            System.Net.ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12;
                            string host_servidor = "";
                            if (es_produccion)
                            {
                                host_servidor = "https://api.sii.cl/recursos/v1/boleta.electronica.envio/76958430-7-";
                            }
                            else
                            {
                                host_servidor = "https://apicert.sii.cl/recursos/v1/boleta.electronica.envio/76958430-7-";
                            }
                            var client = new RestClient(host_servidor + TrackId_str);
                            client.Timeout = -1;
                            var request = new RestRequest(Method.GET);
                            request.AddCookie("Cookie", "TOKEN=" + token);
                            request.AlwaysMultipartFormData = true;
                            IRestResponse response = client.Execute(request);

                            resultadoEnvio = response.Content;

                            if ((resultadoEnvio.StartsWith("{") && resultadoEnvio.EndsWith("}")) || (resultadoEnvio.StartsWith("[") && resultadoEnvio.EndsWith("]")))
                            {
                                //ES UN JSON
                                try
                                {
                                    respuesta_envio_estado_json = JsonConvert.DeserializeObject(resultadoEnvio);
                                    estado_str = respuesta_envio_estado_json["estado"];

                                    //RESCATAR EL CAMPO "detalle_rep_rech" Y GUARDARLO EN LA BD EN UNA COLUMNA NUEVA
                                    detalle_str = resultadoEnvio;



                                }
                                catch (Exception ex)
                                {
                                    Console.ForegroundColor = ConsoleColor.Red;
                                    Console.WriteLine("[" + getFecha() + "]:Hubo un error, respuesta del servidor del SII:" + resultadoEnvio);
                                    Consola_log = Consola_log + "[" + getFecha() + "]:Hubo un error, respuesta del servidor del SII:" + resultadoEnvio + Environment.NewLine;
                                    Console.ForegroundColor = ConsoleColor.Black;

                                }

                            }
                            else
                            {
                                Console.BackgroundColor = ConsoleColor.Red;
                                Console.WriteLine("[" + getFecha() + "]: Hubo un error oh shieeet, respuesta del servidor del SII:" + resultadoEnvio);
                                Consola_log = Consola_log + "[" + getFecha() + "]: Hubo un error oh shieeet, respuesta del servidor del SII: " + resultadoEnvio + Environment.NewLine;
                                Console.BackgroundColor = ConsoleColor.Black;
                            }


                        }

                    }

                    if (servidor == "maullin" || servidor == "palena")
                    {
                        resultadoEnvio = consultarEstadoEnvio(TrackId_str);
                        //string resultadoEnvio es la respuesta en XML


                        xmlDoc.LoadXml(resultadoEnvio);
                        XmlNodeList estado = xmlDoc.GetElementsByTagName("ESTADO");
                        XmlNodeList respuesta_completa = xmlDoc.GetElementsByTagName("SII:RESPUESTA");



                        if (estado.Count == 0)
                        {
                            Console.BackgroundColor = ConsoleColor.Red;
                            Console.WriteLine("[" + getFecha() + "]: Hubo un error oh shieeet, respuesta del servidor del SII:" + resultadoEnvio);
                            Consola_log = Consola_log + "[" + getFecha() + "]: Hubo un error oh shieeet, respuesta del servidor del SII: " + resultadoEnvio + Environment.NewLine;
                            Console.BackgroundColor = ConsoleColor.Black;



                        }
                        else
                        {
                            try
                            {
                                estado_str = estado[0].InnerXml;
                                detalle_str = respuesta_completa[0].InnerXml;

                            }
                            catch (Exception e)
                            {
                                Console.ForegroundColor = ConsoleColor.Red;
                                Console.WriteLine("[" + getFecha() + "]:Hubo un error, respuesta del servidor del SII:" + resultadoEnvio);
                                Consola_log = Consola_log + "[" + getFecha() + "]:Hubo un error, respuesta del servidor del SII:" + resultadoEnvio + Environment.NewLine;
                                Console.ForegroundColor = ConsoleColor.Black;
                            }
                        }
                    }






                    //RESPUESTA DEFAULT
                    if (estado_str != "EPR")
                    {
                        Console.BackgroundColor = ConsoleColor.Red;
                        Console.WriteLine("[" + getFecha() + "]: Hubo un error, respuesta del servidor del SII: " + resultadoEnvio + " Track ID : " + TrackId_str);
                        Consola_log = Consola_log + "[" + getFecha() + "]: Hubo un error, respuesta del servidor del SII: " + resultadoEnvio + " Track ID : " + TrackId_str + Environment.NewLine;
                        Console.BackgroundColor = ConsoleColor.Black;
                        string queryUpdateEstado = "UPDATE envio_dte SET  estado_envio_dte = 'Enviado',  revision_envio_dte = '0', detalle_envio_dte = '" + detalle_str + "'  WHERE trackid_envio_dte = '" + TrackId_str + "';";
                        conexion.Consulta(queryUpdateEstado);

                    }



                    if (estado_str == "-11")
                    {


                        //MANDAR MENSAJE ERROR DE REINTENTOS DE FOLIO REPETIDO
                        string mensaje = "[" + getFecha() + "]: Error: Sobre de envio trackID: " + TrackId_str + ", aun no procesado o con error en SII";
                        Console.BackgroundColor = ConsoleColor.Red;
                        Console.WriteLine(mensaje);
                        Consola_log = Consola_log + mensaje + Environment.NewLine;
                        conexion.Consulta("INSERT INTO log_event (id_log_event, mensaje_log_event, fecha_log_event, referencia_log_event, query_request_log_event) VALUES (NULL, '" + mensaje + "', NOW(), 'TrackID: " + TrackId_str + " Aplicacion de Servicio ASConsultaEstadoEnvio', '') ");
                        mandarEmailSobre(TrackId_str, "Error", detalle_str);

                        string queryUpdateEstado = "UPDATE envio_dte SET  estado_envio_dte = 'Enviado',  revision_envio_dte = '0', detalle_envio_dte = '" + detalle_str + "'  WHERE trackid_envio_dte = '" + TrackId_str + "';";
                        conexion.Consulta(queryUpdateEstado);
                        Console.BackgroundColor = ConsoleColor.Black;


                    }


                    string informados_str = "";
                    string aceptados_str = "";
                    string rechazados_str = "";
                    string reparos_str = "";
                    string consumo_folio_str = "";

                    if (estado_str == "RCT")
                    {
                        //UPDATE EL SOBRE RECHAZADO CON EL ESTADO "RECHAZADO"
                        string queryUpdateEstado = "UPDATE envio_dte SET  estado_envio_dte = 'Enviado', rechazos_envio_dte = '1', informados_envio_dte = '1', revision_envio_dte = '1', detalle_envio_dte = '" + detalle_str + "'  WHERE trackid_envio_dte = '" + TrackId_str + "';";
                        conexion.Consulta(queryUpdateEstado);
                    }

                    if (estado_str == "EPR")
                    {
                        //ENVIO PROCESADO
                        if (servidor == "api" || servidor == "apicert")
                        {
                            respuesta_envio_estado_json = JsonConvert.DeserializeObject(resultadoEnvio);
                            var estadisticas_array = JsonConvert.DeserializeObject(respuesta_envio_estado_json["estadistica"].ToString());
                            string estadisticas_str_json = estadisticas_array[0].ToString();
                            dynamic estadisticas_json = JsonConvert.DeserializeObject(estadisticas_str_json);

                            informados_str = estadisticas_json["informados"].ToString();
                            aceptados_str = estadisticas_json["aceptados"].ToString();
                            rechazados_str = estadisticas_json["rechazados"].ToString();
                            reparos_str = estadisticas_json["reparos"].ToString();




                        }

                        if (servidor == "maullin" || servidor == "palena")
                        {
                            //RESCATAMOS LOS VALORES DE LA RESPUESTA EN XML

                            XmlNodeList informados = xmlDoc.GetElementsByTagName("INFORMADOS");
                            XmlNodeList aceptados = xmlDoc.GetElementsByTagName("ACEPTADOS");
                            XmlNodeList rechazados = xmlDoc.GetElementsByTagName("RECHAZADOS");
                            XmlNodeList reparos = xmlDoc.GetElementsByTagName("REPAROS");

                            if (informados.Count == 0 && aceptados.Count == 0 && rechazados.Count == 0 && reparos.Count == 0)
                            {
                                //QUIERE DECIR QUE ES UNA RESPUESTA DE CONSUMO FOLIOS
                                consumo_folio_str = "1";
                                informados_str = "0";
                                aceptados_str = "0";
                                rechazados_str = "0";
                                reparos_str = "0";
                            }
                            else
                            {
                                informados_str = informados[0].InnerXml;
                                aceptados_str = aceptados[0].InnerXml;
                                rechazados_str = rechazados[0].InnerXml;
                                reparos_str = reparos[0].InnerXml;
                                consumo_folio_str = "0";
                            }



                        }


                        //SI HAY UN RECHAZADO MANDAR MENSAJE DE ERROR
                        if (rechazados_str != "0") //Si es distinto a 0 es porque hay rechazo
                        {

                            //UPDATE EL SOBRE RECHAZADO CON EL ESTADO "RECHAZADO"
                            string queryUpdateEstado = "UPDATE envio_dte SET  estado_envio_dte = 'Enviado', rechazos_envio_dte = '1', informados_envio_dte = '1', revision_envio_dte = '1', detalle_envio_dte = '" + detalle_str + "'  WHERE trackid_envio_dte = '" + TrackId_str + "';";
                            conexion.Consulta(queryUpdateEstado);

                            //CREAMOS LA RESPUESTA DEL RECHAZO
                            string mensaje = "[" + getFecha() + "]: Error: Sobre de envio trackID: " + TrackId_str + ", fue rechazado";
                            Console.ForegroundColor = ConsoleColor.Red;
                            Console.WriteLine(mensaje);
                            Consola_log = Consola_log + mensaje + Environment.NewLine;
                            conexion.Consulta("INSERT INTO log_event (id_log_event, mensaje_log_event, fecha_log_event, referencia_log_event, query_request_log_event) VALUES (NULL, '" + mensaje + "', NOW(), 'TrackID: " + TrackId_str + " Aplicacion de Servicio ASConsultaEstadoEnvio', '') ");
                            Console.ForegroundColor = ConsoleColor.White;
                            mandarEmailSobre(TrackId_str, "Rechazo DTE", detalle_str);
                        }

                        //SI HAY UN REPARO, INFORMAR
                        if (reparos_str != "0") //Si es distinto a 0 es porque hay reparo
                        {
                            //UPDATE EL SOBRE RECHAZADO CON EL ESTADO "RECHAZADO"
                            string queryUpdateEstado = "UPDATE envio_dte SET  estado_envio_dte = 'Enviado', reparos_envio_dte = '1', informados_envio_dte = '1', revision_envio_dte = '1', detalle_envio_dte = '" + detalle_str + "'   WHERE trackid_envio_dte = '" + TrackId_str + "';";
                            conexion.Consulta(queryUpdateEstado);

                            //CREAMOS LA RESPUESTA DEL RECHAZO
                            string mensaje = "[" + getFecha() + "]: Advertencia: Sobre de envio trackID: " + TrackId_str + ", fue enviado con reparo";
                            Console.ForegroundColor = ConsoleColor.Yellow;
                            Console.WriteLine(mensaje);
                            Consola_log = Consola_log + mensaje + Environment.NewLine;
                            conexion.Consulta("INSERT INTO log_event (id_log_event, mensaje_log_event, fecha_log_event, referencia_log_event, query_request_log_event) VALUES (NULL, '" + mensaje + "', NOW(), 'TrackID: " + TrackId_str + " Aplicacion de Servicio ASConsultaEstadoEnvio', '') ");
                            Console.ForegroundColor = ConsoleColor.White;
                            mandarEmailSobre(TrackId_str, "Reparo DTE", detalle_str);
                        }

                        if (aceptados_str != "0")
                        {
                            //XML ACEPTADO MANDAR MENSAJE DE EXITO
                            //UPDATE EL SOBRE RECHAZADO CON EL ESTADO "RECHAZADO"
                            string queryUpdateEstado = "UPDATE envio_dte SET  estado_envio_dte = 'Enviado', aceptados_envio_dte = '1', informados_envio_dte = '1', revision_envio_dte = '1',detalle_envio_dte = '" + detalle_str + "'  WHERE trackid_envio_dte = '" + TrackId_str + "';";
                            conexion.Consulta(queryUpdateEstado);

                            //string mensaje = "Aceptado: Sobre de envio trackID: " + TrackId_str + "";
                            //Console.WriteLine(mensaje);


                        }

                        if (consumo_folio_str != "0")
                        {
                            string queryUpdateEstado = "UPDATE envio_dte SET  estado_envio_dte = 'Enviado', aceptados_envio_dte = '1', informados_envio_dte = '1',envio_cliente_envio_dte = '2', revision_envio_dte = '1',detalle_envio_dte = '" + detalle_str + "'  WHERE trackid_envio_dte = '" + TrackId_str + "';";
                            conexion.Consulta(queryUpdateEstado);
                        }
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine("[" + getFecha() + "]: HUBO ERROR EN UpdateEstadoSobre() :" + ex.Message);
                    Consola_log = Consola_log + "[" + getFecha() + "]: HUBO ERROR EN UpdateEstadoSobre() :" + ex.Message + Environment.NewLine;

                }
            }
            else
            {
                Console.WriteLine("[" + getFecha() + "]: HUBO ERROR CON EL TRACKID UpdateEstadoSobre() :" + TrackId_str);
                Consola_log = Consola_log + "[" + getFecha() + "]: HUBO ERROR CON EL TRACKID UpdateEstadoSobre() :" + TrackId_str + Environment.NewLine;
            }
        }

        //INVOCADA POR updateEstadoSobre() CONSULTA EL ESTADO DEL SOBRE DE FACTURAS O DTE A MAULLIN O PALENA
        public static string consultarEstadoEnvio(string trackID)
        {
            string path_servicio_consulta_envio = "";
            if (es_produccion)
            {
                path_servicio_consulta_envio = "http://192.168.1.9:90/WebServiceEnvioDTE/EnvioSobreDTE.asmx/consultarEstadoEnvio?Numero_Envio=" + trackID;
            }
            else
            {
                //path_servicio_consulta_envio = "http://localhost:81/WebServiceEnvioDTE_Maullin/EnvioSobreDTE.asmx/consultarEstadoEnvio?Numero_Envio=" + trackID;
                path_servicio_consulta_envio = "http://192.168.1.9:90/WebServiceEnvioDTE_Maullin/EnvioSobreDTE.asmx/consultarEstadoEnvio?Numero_Envio=" + trackID;

            }

            //WebRequest request = WebRequest.Create("https://localhost:44327/EnvioSobreDTE.asmx/consultarEstadoEnvio?Numero_Envio=" + trackID);
            WebRequest request = WebRequest.Create(path_servicio_consulta_envio);
            request.Method = "GET";
            WebResponse response = request.GetResponse();
            string respuestaEnvio = "";
            using (var reader = new StreamReader(response.GetResponseStream()))
            {
                respuestaEnvio = reader.ReadToEnd(); // do something fun...
            }

            respuestaEnvio = respuestaEnvio.Replace(@"<?xml version=""1.0"" encoding=""utf-8""?>", String.Empty);
            respuestaEnvio = respuestaEnvio.Replace(@"<string xmlns=""http://tempuri.org/"">", "");
            respuestaEnvio = respuestaEnvio.Replace(@"</string>", "");
            respuestaEnvio = respuestaEnvio.Replace("&lt;", "<");
            respuestaEnvio = respuestaEnvio.Replace("&gt;", ">");
            respuestaEnvio = respuestaEnvio.Replace("\n", String.Empty);
            respuestaEnvio = respuestaEnvio.Replace("\r", String.Empty);
            respuestaEnvio = respuestaEnvio.Replace("\t", String.Empty);
            return respuestaEnvio;
        }

        //CHEQUEA SI HAY SALIDA A INTERNET
        public static string checkPing(string host)
        {
            try
            {
                Ping myPing = new Ping();
                // String host = "192.168.1.6";//agroplastic.cl
                byte[] buffer = new byte[32];
                int timeout = 10000;
                PingOptions pingOptions = new PingOptions();
                PingReply reply = myPing.Send(host, timeout, buffer, pingOptions);
                //Console.WriteLine(reply.Status == IPStatus.Success);

                if (reply.Status == IPStatus.Success)
                {
                    return "Conexion Exitosa";
                }
            }
            catch (Exception e)
            {

                return "Hubo Error en ping - " + e.Message;
            }

            return "";
        }

        //ENVIA UN EMAIL EN CASO QUE ALGUN DOCUMENTO DTE SALIO CON REPARO O RECHAZO
        public static void mandarEmailSobre(string TrackID,string asunto,string detalle)
        {
            string mensajeCorreo = "";

            switch (asunto)
            {

                case "Rechazo DTE":
                    mensajeCorreo = @"<h4><span style=""color: #000000;"">Se ha detectado un error o rechazo en sobre trackid #"+TrackID+"</span></h4><p>Detalle: "+detalle+"</p>";
                    break;
                case "Reparo DTE":
                    mensajeCorreo = @"<h4><span style=""color: #000000;"">Se ha detectado un error o reparo en sobre trackid #"+TrackID+ "</span></h4><p>Detalle: " + detalle + "</p>";
                    break;

                default:
                    mensajeCorreo = @"<h4><span style=""color: #000000;"">Se ha detectado un error de algun tipo en sobre trackid #" + TrackID + "</span></h4><p>Detalle: " + detalle + "</p>";
                    break;
            }
            try
            {

                SmtpClient mySmtpClient = new SmtpClient("mail.agroplastic.cl");

                



                // set smtp-client with basicAuthentication
                mySmtpClient.UseDefaultCredentials = false;
                System.Net.NetworkCredential basicAuthenticationInfo = new
                   System.Net.NetworkCredential(MailSistema, PassSistema);
                mySmtpClient.Credentials = basicAuthenticationInfo;

                // add from,to mailaddresses
                MailAddress from = new MailAddress(MailSistema, "AgroDTE");
                //MailAddress to = new MailAddress("bmcortes@agroplastic.cl, mriquelme@agroplastic.cl", "Benjamin");
                MailMessage myMail = new MailMessage();
                myMail.To.Add("bmcortes@agroplastic.cl");
                myMail.To.Add("mriquelme@agroplastic.cl");
                myMail.From = from;



                // add ReplyTo
                //MailAddress replyTo = new MailAddress("mriquelme@agroplastic.cl");
                //myMail.ReplyToList.Add(replyTo);

                // set subject and encoding
                myMail.Subject = asunto;
                myMail.SubjectEncoding = System.Text.Encoding.UTF8;

                // set body-message and encoding
                myMail.Body = mensajeCorreo;
                myMail.BodyEncoding = System.Text.Encoding.UTF8;
                // text or html
                myMail.IsBodyHtml = true;

                mySmtpClient.Send(myMail);
            }

            catch (SmtpException ex)
            {
                throw new ApplicationException
                  ("SmtpException has occured: " + ex.Message);
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        //MANDAR EMAIL INFORMANDO LA PRONTA FALTA DE CAF
        public static void mandarEmailCaf(string tipo_dte, int diferencia)
        {
            Bitmap bitmap = new Bitmap(@"C:\inetpub\wwwroot\api_agrodte\AgroDTE_Archivos\logo_AgroDTE_OFICIAL.png");
            System.IO.MemoryStream ms = new MemoryStream();
            bitmap.Save(ms, ImageFormat.Png);
            byte[] byteImage = ms.ToArray();
            var SigBase64 = Convert.ToBase64String(byteImage);

            string mensajeCorreo = @"<h4><span style=""color: #000000;"">Se ha detectado que quedan pocos folios en el siguiente DTE:</span></h4>";
           mensajeCorreo = mensajeCorreo +  @"<h4><span style=""color: #000000;"">En el dte "+tipo_dte+" quedan "+diferencia+" folios </span></h4>";
            mensajeCorreo = mensajeCorreo + @"<img src=""data:image/png;base64," + SigBase64 + @"""    width=""220"" height=""100"">";
           string asunto = "SOLICITAR FOLIOS";
            try
            {

                SmtpClient mySmtpClient = new SmtpClient("mail.agroplastic.cl");

                // set smtp-client with basicAuthentication
                mySmtpClient.UseDefaultCredentials = false;
                System.Net.NetworkCredential basicAuthenticationInfo = new
                   System.Net.NetworkCredential(MailSistema, PassSistema);
                mySmtpClient.Credentials = basicAuthenticationInfo;

                // add from,to mailaddresses
                System.Net.Mail.MailAddress from = new System.Net.Mail.MailAddress(MailSistema, "AgroDTE");
                //MailAddress to = new MailAddress("bmcortes@agroplastic.cl, mriquelme@agroplastic.cl", "Benjamin");
                MailMessage myMail = new MailMessage();
                myMail.To.Add("bmcortes@agroplastic.cl");
                myMail.To.Add("mriquelme@agroplastic.cl");
                myMail.To.Add("agrodte@agroplastic.cl");
                myMail.From = from;



                // add ReplyTo
                //MailAddress replyTo = new MailAddress("mriquelme@agroplastic.cl");
                //myMail.ReplyToList.Add(replyTo);

                // set subject and encoding
                myMail.Subject = asunto;
                myMail.SubjectEncoding = System.Text.Encoding.UTF8;

                // set body-message and encoding
                myMail.Body = mensajeCorreo;
                myMail.BodyEncoding = System.Text.Encoding.UTF8;
                // text or html
                myMail.IsBodyHtml = true;

                mySmtpClient.Send(myMail);
            }

            catch (SmtpException ex)
            {
                throw new ApplicationException
                  ("SmtpException has occured: " + ex.Message);
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        // INVOCA AL MICROSERICIO DE ENVIO DE SOBRES
        public static string enviarSobre(string archivo, string rutEmisor, string rutEmpresa)
        {
            string path_servicio_envio = "";
            if (es_produccion)
            {
                path_servicio_envio = "http://192.168.1.9:90/WebServiceEnvioDTE/EnvioSobreDTE.asmx/enviarSobreSII?archivo=" + archivo + "&rutEmisor=" + rutEmisor + "&rutEmpresa=" + rutEmpresa;
            }
            else
            {
                path_servicio_envio = "http://192.168.1.9:90/WebServiceEnvioDTE_Maullin/EnvioSobreDTE.asmx/enviarSobreSII?archivo=" + archivo + "&rutEmisor=" + rutEmisor + "&rutEmpresa=" + rutEmpresa;
                //path_servicio_envio = "https://localhost:443/WebServiceEnvioDTE/EnvioSobreDTE.asmx/enviarSobreSII?archivo=" + archivo + "&rutEmisor=" + rutEmisor + "&rutEmpresa=" + rutEmpresa;
            }
       
            //System.Net.ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12;
            //WebRequest request = WebRequest.Create("https://localhost:44327/EnvioSobreDTE.asmx/enviarSobreSII?archivo=" + archivo + "&rutEmisor=" + rutEmisor + "&rutEmpresa=" + rutEmpresa);
            WebRequest request = WebRequest.Create(path_servicio_envio);
            request.Method = "GET";
            WebResponse response = request.GetResponse();
            string respuestaEnvio = "";
            using (var reader = new StreamReader(response.GetResponseStream()))
            {
                respuestaEnvio = reader.ReadToEnd(); // do something fun...
            }
            return respuestaEnvio;
        }

        //CREA EL DIRECTORIO PARA ALMACENAR EL XML DEL CONSUMO DE FOLIOS BOLETAS
        public static string crearDirectorio( string T33F)
        {



            //Crear ruta del archivo xml saliente
            string currentDay = DateTime.Now.Day.ToString();
            string currentMonth = DateTime.Now.Month.ToString();
            string currentYear = DateTime.Now.Year.ToString();
            string archxml = "";
            return archxml = verificarDirectorio(T33F, currentYear, "M" + currentMonth, "D" + currentDay);
        }

        //VERIFICA EL DIRECTORIO DEL CONSUMO DE FOLIOS DE BOLETAS
        public static string verificarDirectorio(string nombreDTE, string year, string mes, string dia)
        {

            //VERIFICAR AÑO
            string folderPathYear = directorio_archivos + @"XML\" + year;

            if (!Directory.Exists(folderPathYear))
            {
                Directory.CreateDirectory(folderPathYear);
            }
            //VERIFICAR MES
            string folderPathMes = directorio_archivos + @"XML\" + year + @"\" + mes;
            if (!Directory.Exists(folderPathMes))
            {
                Directory.CreateDirectory(folderPathMes);
            }

            //VERIFICAR DIA
            string folderPathDia = directorio_archivos + @"XML\" + year + @"\" + mes + @"\" + dia;
            if (!Directory.Exists(folderPathDia))
            {
                Directory.CreateDirectory(folderPathDia);
            }


            return folderPathDia;
        }
       


    }
}
