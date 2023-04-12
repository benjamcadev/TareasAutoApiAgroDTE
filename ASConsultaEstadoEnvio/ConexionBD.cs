using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using MySqlConnector;

namespace TareasProgramadasAgroDTE
{
    public class ConexionBD
    {

        //CREARR SWITCH SI ES DE PRUEBA O PRODUCCION

        static string servidor = "192.168.1.9";
        static string bd = "dte_agroplastic";
        static string usuario = "root"; 
        static string password = "agrodte1113"; 
        static string port ="3307";
        static string cadenaConexion = "Database=" + bd + "; Data Source=" + servidor + "; User Id=" + usuario + "; Password=" + password + ";Port="+port+ ";Allow User Variables=True";

        public List<string> Select(string consulta){
            
            List<string> datosLista = new List<string> ();
            
            MySqlConnection conexionBD = new MySqlConnection(cadenaConexion);
            MySqlDataReader reader = null; //Variable para leer el resultado de la consulta
    
                //Agregamos try-catch para capturar posibles errores de conexión o sintaxis.
            try
            {
                //string consulta = "SHOW DATABASES"; //Consulta a MySQL (Muestra las bases de datos que tiene el servidor)
                MySqlCommand comando = new MySqlCommand(consulta); //Declaración SQL para ejecutar contra una base de datos MySQL
                comando.Connection = conexionBD; //Establece la MySqlConnection utilizada por esta instancia de MySqlCommand
                conexionBD.Open(); //Abre la conexión
                reader = comando.ExecuteReader(); //Ejecuta la consulta y crea un MySqlDataReader
                
                int count = reader.FieldCount;    

                while (reader.Read()) //Avanza MySqlDataReader al siguiente registro
                {

                    for(int i = 0 ; i < count ; i++) 
                    {
                        datosLista.Add(reader.GetValue(i).ToString());
                    }

                                     

                   // datos += reader.GetString(0)+","; //Almacena cada registro con un salto de linea
                }
    
               // MessageBox.Show(datos); //Imprime en cuadro de dialogo el resultado
            }
            catch (MySqlException ex)
            {
                //MessageBox.Show(ex.Message); //Si existe un error aquí muestra el mensaje
            }
            finally
            {
                conexionBD.Close(); //Cierra la conexión a MySQL
            }

           
            return datosLista;
        }
        public List<string> Select_Json(string consulta)
        {

            List<string> datosLista = new List<string>();

            MySqlConnection conexionBD = new MySqlConnection(cadenaConexion);
            MySqlDataReader reader = null; //Variable para leer el resultado de la consulta

            //Agregamos try-catch para capturar posibles errores de conexión o sintaxis.
            try
            {
                //string consulta = "SHOW DATABASES"; //Consulta a MySQL (Muestra las bases de datos que tiene el servidor)
                MySqlCommand comando = new MySqlCommand(consulta); //Declaración SQL para ejecutar contra una base de datos MySQL
                comando.Connection = conexionBD; //Establece la MySqlConnection utilizada por esta instancia de MySqlCommand
                conexionBD.Open(); //Abre la conexión
                reader = comando.ExecuteReader(); //Ejecuta la consulta y crea un MySqlDataReader

                int count = reader.FieldCount;


                string json_response = "";

                while (reader.Read()) //Avanza MySqlDataReader al siguiente registro
                {

                    for (int i = 0; i < count; i++)
                    {
                      datosLista.Add(reader.GetValue(i).ToString());
                    }



                    // datos += reader.GetString(0)+","; //Almacena cada registro con un salto de linea
                }

                // MessageBox.Show(datos); //Imprime en cuadro de dialogo el resultado
            }
            catch (MySqlException ex)
            {
                //MessageBox.Show(ex.Message); //Si existe un error aquí muestra el mensaje
            }
            finally
            {
                conexionBD.Close(); //Cierra la conexión a MySQL
            }


            return datosLista;
        }

        public List<string> SelectDatosempresa(string consulta)
        {

            List<string> datosLista = new List<string>();

            MySqlConnection conexionBD = new MySqlConnection(cadenaConexion);
            MySqlDataReader reader = null; //Variable para leer el resultado de la consulta

            //Agregamos try-catch para capturar posibles errores de conexión o sintaxis.
            try
            {
                //string consulta = "SHOW DATABASES"; //Consulta a MySQL (Muestra las bases de datos que tiene el servidor)
                MySqlCommand comando = new MySqlCommand(consulta); //Declaración SQL para ejecutar contra una base de datos MySQL
                comando.Connection = conexionBD; //Establece la MySqlConnection utilizada por esta instancia de MySqlCommand
                conexionBD.Open(); //Abre la conexión
                reader = comando.ExecuteReader(); //Ejecuta la consulta y crea un MySqlDataReader

                int count = 1;

                while (reader.Read()) //Avanza MySqlDataReader al siguiente registro
                {

                    for (int i = 0; i < count; i++)
                    {
                       
                        datosLista.Add(reader.GetString(0).ToString());
                        datosLista.Add(reader.GetString(1).ToString());
                        datosLista.Add(reader.GetValue(2).ToString());
                        datosLista.Add(reader.GetInt32(3).ToString());
                        datosLista.Add(reader.GetString(4).ToString());
                        datosLista.Add(reader.GetString(5).ToString());
                        datosLista.Add(reader.GetString(6).ToString());

                    }



                    // datos += reader.GetString(0)+","; //Almacena cada registro con un salto de linea
                }

                // MessageBox.Show(datos); //Imprime en cuadro de dialogo el resultado
            }
            catch (MySqlException ex)
            {
                //MessageBox.Show(ex.Message); //Si existe un error aquí muestra el mensaje
            }
            finally
            {
                conexionBD.Close(); //Cierra la conexión a MySQL
            }


            return datosLista;
        }


        public List<string> SelectFecha(string consulta)
        {

            List<string> datosLista = new List<string>();

            MySqlConnection conexionBD = new MySqlConnection(cadenaConexion);
            MySqlDataReader reader = null; //Variable para leer el resultado de la consulta

            //Agregamos try-catch para capturar posibles errores de conexión o sintaxis.
            try
            {
                //string consulta = "SHOW DATABASES"; //Consulta a MySQL (Muestra las bases de datos que tiene el servidor)
                MySqlCommand comando = new MySqlCommand(consulta); //Declaración SQL para ejecutar contra una base de datos MySQL
                comando.Connection = conexionBD; //Establece la MySqlConnection utilizada por esta instancia de MySqlCommand
                conexionBD.Open(); //Abre la conexión
                reader = comando.ExecuteReader(); //Ejecuta la consulta y crea un MySqlDataReader

                int count = reader.FieldCount;

                while (reader.Read()) //Avanza MySqlDataReader al siguiente registro
                {

                    for (int i = 0; i < count; i++)
                    {
                        datosLista.Add(reader.GetDateTime(0).ToString());
                        datosLista.Add(reader.GetInt32(1).ToString());
                        datosLista.Add(reader.GetString(2).ToString());
                    }



                    // datos += reader.GetString(0)+","; //Almacena cada registro con un salto de linea
                }

                // MessageBox.Show(datos); //Imprime en cuadro de dialogo el resultado
            }
            catch (MySqlException ex)
            {
                //MessageBox.Show(ex.Message); //Si existe un error aquí muestra el mensaje
            }
            finally
            {
                conexionBD.Close(); //Cierra la conexión a MySQL
            }


            return datosLista;
        }

        public List<string> SelectString(string consulta)
        {

            List<string> datosLista = new List<string>();

            MySqlConnection conexionBD = new MySqlConnection(cadenaConexion);
            MySqlDataReader reader = null; //Variable para leer el resultado de la consulta

            //Agregamos try-catch para capturar posibles errores de conexión o sintaxis.
            try
            {
                //string consulta = "SHOW DATABASES"; //Consulta a MySQL (Muestra las bases de datos que tiene el servidor)
                MySqlCommand comando = new MySqlCommand(consulta); //Declaración SQL para ejecutar contra una base de datos MySQL
                comando.Connection = conexionBD; //Establece la MySqlConnection utilizada por esta instancia de MySqlCommand
                conexionBD.Open(); //Abre la conexión
                reader = comando.ExecuteReader(); //Ejecuta la consulta y crea un MySqlDataReader

                int count = reader.FieldCount;

                while (reader.Read()) //Avanza MySqlDataReader al siguiente registro
                {

                    for (int i = 0; i < count; i++)
                    {
                       
                        datosLista.Add(reader.GetString(0).ToString());
                    }



                    // datos += reader.GetString(0)+","; //Almacena cada registro con un salto de linea
                }

                // MessageBox.Show(datos); //Imprime en cuadro de dialogo el resultado
            }
            catch (MySqlException ex)
            {
                //MessageBox.Show(ex.Message); //Si existe un error aquí muestra el mensaje
            }
            finally
            {
                conexionBD.Close(); //Cierra la conexión a MySQL
            }


            return datosLista;
        }

        public void Consulta(string consulta){

                                   
            MySqlConnection conexionBD = new MySqlConnection(cadenaConexion);
            MySqlDataReader reader = null; 
    
            try
            {
                MySqlCommand comando = new MySqlCommand(consulta); 
                comando.Connection = conexionBD; 
                conexionBD.Open(); 
                reader = comando.ExecuteReader();
                
                while (reader.Read()) 
                {

                }
    
               
            }
            catch (MySqlException ex)
            {
                
            }
            finally
            {
                conexionBD.Close(); 
            }
        }
    }
}
