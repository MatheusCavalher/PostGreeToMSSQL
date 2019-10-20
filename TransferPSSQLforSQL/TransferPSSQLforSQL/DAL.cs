using System;
using System.Data;
using Npgsql;

namespace TransferPSSQLforSQL
{
    class DAL
    {
        static string serverName = "";   //localhost
        static string port = "";                  //porta default
        static string userName = "";          //nome do administrador
        static string password = "";          //senha do administrador
        static string databaseName = "";       //nome do banco de dados
        static int commandTimeout = 0;                //setando timeout para query infinita
        NpgsqlConnection pgsqlConnection = null;
        string connString = null;

        public DAL()
        {
            connString = String.Format("Server={0};Port={1};Database={2};User Id={3};Password={4};CommandTimeout={5}",
                                       serverName, port, databaseName, userName, password, commandTimeout);
        }

        public DataTable GetTodosRegistros()
        {
            DataTable dt = new DataTable();

            try
            {
                using (pgsqlConnection = new NpgsqlConnection(connString))
                {
                    //abre a conexão com o PostGreeSQL e define a instrução SQL
                    pgsqlConnection.Open();
                    string cmdSeleciona = "SELECT * FROM  where date_start >= CURRENT_DATE";

                    using (NpgsqlDataAdapter adpt = new NpgsqlDataAdapter(cmdSeleciona, pgsqlConnection))
                    {
                        adpt.Fill(dt);
                    }
                }
            }
            catch(NpgsqlException ex) 
            {
                Console.Write("Não foi possivel trazer os registros da tabela\n");
                throw ex; 
            }
            catch (Exception ex) 
            {
                Console.Write("Não foi possivel trazer os registros da tabela\n");
                throw ex; 
            }
            finally 
            {
                pgsqlConnection.Close(); 
            }

            return dt;
        }

    }
}
