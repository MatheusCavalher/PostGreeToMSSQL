using System;
using System.Data;
using System.Data.SqlClient;

namespace TransferPSSQLforSQL
{
    class DALSQL
    {
        private static SqlConnection conn = null;


        public static SqlConnection conectarBanco()
        {
            string connString = "SERVER = ; USER ID = ; PASSWORD =; DATABASE = ";
            conn = new SqlConnection(connString);
            try
            {
                if (conn.State == ConnectionState.Closed)
                {
                    conn.Open();
                }
            }
            catch
            {
                string connString2 = "SERVER = ; USER ID = ; PASSWORD = ; DATABASE = ";
                conn = new SqlConnection(connString2);

                if (conn.State == ConnectionState.Closed)
                {
                    conn.Open();
                }
            }
            return conn;
        }
        public static void desconectarBanco()
        {
            if (conn != null)
            {
                conn.Close();
                conn.Dispose();
            }
        }


        public void inserirDados(string _channel, string _ani, string _agente, string _extension, string _dnis, string _virtual_group, string _outgoing_call, DateTime _date_start, DateTime _date_end, DateTime _acd_start, DateTime _agente_start, DateTime _agente_end, string _modo, string _atendida, string _record, string _a_agente, string _v_score, string _conv, string _dur_file, string _tab_1, string _tab_2, string _h_agente, string _d_uniqueid, string _dialstatus, DateTime _dialinidate)
        {
            try
            {
                SqlCommand cmd;
                SqlConnection conn;

                //abre conexão com banco de dados
                conn = conectarBanco();

                //cria query para inserir no banco de dados
                String query = "INSERT INTO (channel, ani,  agente,  extension,  dnis,  virtual_group,  outgoing_call,  date_start,  date_end,  acd_start,  agente_start,  agente_end,  modo,  atendida,  record,  a_agente,  v_score,  conv,  dur_file,  tab_1,  tab_2,  h_agente,  d_uniqueid,  dialstatus,  dialinidate, data_modificacao) VALUES " +
                                                                         "(@channel,@ani, @agente, @extension, @dnis, @virtual_group, @outgoing_call, @date_start, @date_end, @acd_start, @agente_start, @agente_end, @modo, @atendida, @record, @a_agente, @v_score, @conv, @dur_file, @tab_1, @tab_2, @h_agente, @d_uniqueid, @dialstatus, @dialinidate, SYSDATETIME())";
                cmd = new SqlCommand(query);

                cmd.Connection = conn;

                //trata dados nulos
                if (_channel == null || _channel.Equals(""))
                    cmd.Parameters.AddWithValue("@channel", Type.GetType("System.String")).Value = System.DBNull.Value;
                else
                    cmd.Parameters.AddWithValue("@channel", Type.GetType("System.String")).Value = _channel;


                if (_ani == null || _ani.Equals(""))
                    cmd.Parameters.AddWithValue("@ani", Type.GetType("System.String")).Value = System.DBNull.Value;
                else
                    cmd.Parameters.AddWithValue("@ani", Type.GetType("System.String")).Value = _ani;


                if (_agente == null || _agente.Equals(""))
                    cmd.Parameters.AddWithValue("@agente", Type.GetType("System.String")).Value = System.DBNull.Value;
                else
                    cmd.Parameters.AddWithValue("@agente", Type.GetType("System.String")).Value = _agente;


                if (_extension == null || _extension.Equals(""))
                    cmd.Parameters.AddWithValue("@extension", Type.GetType("System.String")).Value = System.DBNull.Value;
                else
                    cmd.Parameters.AddWithValue("@extension", Type.GetType("System.String")).Value = _extension;


                if (_dnis == null || _dnis.Equals(""))
                    cmd.Parameters.AddWithValue("@dnis", Type.GetType("System.String")).Value = System.DBNull.Value;
                else
                    cmd.Parameters.AddWithValue("@dnis", Type.GetType("System.String")).Value = _dnis;


                if (_virtual_group == null || _virtual_group.Equals(""))
                    cmd.Parameters.AddWithValue("@virtual_group", Type.GetType("System.String")).Value = System.DBNull.Value;
                else
                    cmd.Parameters.AddWithValue("@virtual_group", Type.GetType("System.String")).Value = _virtual_group;


                if (_outgoing_call == null || _outgoing_call.Equals(""))
                    cmd.Parameters.AddWithValue("@outgoing_call", Type.GetType("System.String")).Value = System.DBNull.Value;
                else
                    cmd.Parameters.AddWithValue("@outgoing_call", Type.GetType("System.String")).Value = _outgoing_call;


                if (_date_start.Day == 01 && _date_start.Month == 01 && _date_start.Year == 0001)
                {
                    cmd.Parameters.AddWithValue("@date_start", Type.GetType("System.Date")).Value = System.DBNull.Value;
                }
                else
                {
                    cmd.Parameters.AddWithValue("@date_start", Type.GetType("System.Date")).Value = _date_start;
                }


                if (_date_end.Day == 01 && _date_end.Month == 01 && _date_end.Year == 0001)
                {
                    cmd.Parameters.AddWithValue("@date_end", Type.GetType("System.Date")).Value = System.DBNull.Value;
                }
                else
                {
                    cmd.Parameters.AddWithValue("@date_end", Type.GetType("System.Date")).Value = _date_start;
                }


                if (_acd_start.Day == 01 && _acd_start.Month == 01 && _acd_start.Year == 0001)
                {
                    cmd.Parameters.AddWithValue("@acd_start", Type.GetType("System.Date")).Value = System.DBNull.Value;
                }
                else
                {
                    cmd.Parameters.AddWithValue("@acd_start", Type.GetType("System.Date")).Value = _date_start;
                }


                if (_agente_start.Day == 01 && _agente_start.Month == 01 && _agente_start.Year == 0001)
                {
                    cmd.Parameters.AddWithValue("@agente_start", Type.GetType("System.Date")).Value = System.DBNull.Value;
                }
                else
                {
                    cmd.Parameters.AddWithValue("@agente_start", Type.GetType("System.Date")).Value = _date_start;
                }


                if (_agente_end.Day == 01 && _agente_end.Month == 01 && _agente_end.Year == 0001)
                {
                    cmd.Parameters.AddWithValue("@agente_end", Type.GetType("System.Date")).Value = System.DBNull.Value;
                }
                else
                {
                    cmd.Parameters.AddWithValue("@agente_end", Type.GetType("System.Date")).Value = _date_start;
                }


                if (_modo == null || _modo.Equals(""))
                    cmd.Parameters.AddWithValue("@modo", Type.GetType("System.String")).Value = System.DBNull.Value;
                else
                    cmd.Parameters.AddWithValue("@modo", Type.GetType("System.String")).Value = _modo;


                if (_atendida == null || _atendida.Equals(""))
                    cmd.Parameters.AddWithValue("@atendida", Type.GetType("System.String")).Value = System.DBNull.Value;
                else
                    cmd.Parameters.AddWithValue("@atendida", Type.GetType("System.String")).Value = _atendida;


                if (_record == null || _record.Equals(""))
                    cmd.Parameters.AddWithValue("@record", Type.GetType("System.String")).Value = System.DBNull.Value;
                else
                    cmd.Parameters.AddWithValue("@record", Type.GetType("System.String")).Value = _record;


                if (_a_agente == null || _a_agente.Equals(""))
                    cmd.Parameters.AddWithValue("@a_agente", Type.GetType("System.String")).Value = System.DBNull.Value;
                else
                    cmd.Parameters.AddWithValue("@a_agente", Type.GetType("System.String")).Value = _a_agente;


                if (_v_score == null || _v_score.Equals(""))
                    cmd.Parameters.AddWithValue("@v_score", Type.GetType("System.String")).Value = System.DBNull.Value;
                else
                    cmd.Parameters.AddWithValue("@v_score", Type.GetType("System.String")).Value = _v_score;


                if (_conv == null || _conv.Equals(""))
                    cmd.Parameters.AddWithValue("@conv", Type.GetType("System.String")).Value = System.DBNull.Value;
                else
                    cmd.Parameters.AddWithValue("@conv", Type.GetType("System.String")).Value = _conv;


                if (_dur_file == null || _dur_file.Equals(""))
                    cmd.Parameters.AddWithValue("@dur_file", Type.GetType("System.String")).Value = System.DBNull.Value;
                else
                    cmd.Parameters.AddWithValue("@dur_file", Type.GetType("System.String")).Value = _dur_file;


                if (_tab_1 == null || _tab_1.Equals(""))
                    cmd.Parameters.AddWithValue("@tab_1", Type.GetType("System.String")).Value = System.DBNull.Value;
                else
                    cmd.Parameters.AddWithValue("@tab_1", Type.GetType("System.String")).Value = _tab_1;


                if (_tab_2 == null || _tab_2.Equals(""))
                    cmd.Parameters.AddWithValue("@tab_2", Type.GetType("System.String")).Value = System.DBNull.Value;
                else
                    cmd.Parameters.AddWithValue("@tab_2", Type.GetType("System.String")).Value = _tab_2;


                if (_h_agente == null || _h_agente.Equals(""))
                    cmd.Parameters.AddWithValue("@h_agente", Type.GetType("System.String")).Value = System.DBNull.Value;
                else
                    cmd.Parameters.AddWithValue("@h_agente", Type.GetType("System.String")).Value = _h_agente;


                if (_d_uniqueid == null || _d_uniqueid.Equals(""))
                    cmd.Parameters.AddWithValue("@d_uniqueid", Type.GetType("System.String")).Value = System.DBNull.Value;
                else
                    cmd.Parameters.AddWithValue("@d_uniqueid", Type.GetType("System.String")).Value = _d_uniqueid;


                if (_dialstatus == null || _dialstatus.Equals(""))
                    cmd.Parameters.AddWithValue("@dialstatus", Type.GetType("System.String")).Value = System.DBNull.Value;
                else
                    cmd.Parameters.AddWithValue("@dialstatus", Type.GetType("System.String")).Value = _dialstatus;


                if (_dialinidate.Day == 01 && _dialinidate.Month == 01 && _dialinidate.Year == 0001)
                {
                    cmd.Parameters.AddWithValue("@dialinidate", Type.GetType("System.Date")).Value = System.DBNull.Value;
                }
                else
                {
                    cmd.Parameters.AddWithValue("@dialinidate", Type.GetType("System.Date")).Value = _dialinidate;
                }

                cmd.ExecuteNonQuery();

            }
            catch (SqlException ex)
            {
                Console.Write("Não foi possivel inserir os registros da tabela\n");
                
            }
            catch (Exception ex)
            {
                Console.Write("Não foi possivel inserir os registros da tabela\n");
           
            }
            finally
            {               
                desconectarBanco();
            }
        }

        public void deletarDados()
        {
            try
            {
                SqlCommand cmd;
                SqlDataReader leitor;
                SqlConnection conn;

                //abre conexão com banco de dados
                conn = conectarBanco();
                cmd = new SqlCommand("DELETE FROM  WHERE DAY(DATE_START)=DAY(GETDATE()) AND MONTH(DATE_START)=MONTH(GETDATE()) AND YEAR(DATE_START)=YEAR(GETDATE())");
                cmd.Connection = conn;
                leitor = cmd.ExecuteReader();

            }
            catch (SqlException ex)
            {
                Console.Write("Não foi possivel excluir os registros da tabela\n");
                throw ex;
            }
            catch (Exception ex)
            {
                Console.Write("Não foi possivel excluir os registros da tabela\n");
                throw ex;
            }
            finally
            {
                desconectarBanco();
            }
        }
    }
}
