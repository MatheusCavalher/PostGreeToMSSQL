using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;

namespace TransferPSSQLforSQL
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.Write("Conectando ao banco de dados PostGree\n");
            DAL acessoPostGree = new DAL();

            Console.Write("Selecionando registros da tabela\n");
            DataTable tabela = acessoPostGree.GetTodosRegistros();

            string  _channel, _ani, _agente, _extension, _dnis, _virtual_group, _outgoing_call, _modo, _atendida, _record, _a_agente, _v_score, _conv, _dur_file, _tab_1, _tab_2, _h_agente, _d_uniqueid, _dialstatus;
            DateTime _date_start, _date_end, _acd_start, _agente_start, _agente_end, _dialinidate;

            Console.Write("Apagando registros do SQLSERVER\n");
            DALSQL acessoSQL = new DALSQL();
            acessoSQL.deletarDados();

            Console.Write("Inserindo dados coletados na tabela\n");
            foreach (DataRow dr in tabela.Rows)
            {
                _channel = dr["channel"].Equals(System.DBNull.Value) ? null : dr["channel"].ToString();
                _ani = dr["ani"].Equals(System.DBNull.Value) ? null : dr["ani"].ToString();
                _agente = dr["agente"].Equals(System.DBNull.Value) ? null : dr["agente"].ToString();
                _extension = dr["extension"].Equals(System.DBNull.Value) ? null : dr["extension"].ToString();
                _dnis = dr["dnis"].Equals(System.DBNull.Value) ? null : dr["dnis"].ToString();
                _virtual_group = dr["virtual_group"].Equals(System.DBNull.Value) ? null : dr["virtual_group"].ToString();
                _outgoing_call = dr["outgoing_call"].Equals(System.DBNull.Value) ? null : dr["outgoing_call"].ToString();
                _date_start = dr["date_start"].Equals(System.DBNull.Value) ? new DateTime() : Convert.ToDateTime(dr["date_start"].ToString());
                _date_end = dr["date_end"].Equals(System.DBNull.Value) ? new DateTime() : Convert.ToDateTime(dr["date_end"].ToString());
                _acd_start = dr["acd_start"].Equals(System.DBNull.Value) ? new DateTime() : Convert.ToDateTime(dr["acd_start"].ToString());
                _agente_start = dr["agente_start"].Equals(System.DBNull.Value) ? new DateTime() : Convert.ToDateTime(dr["agente_start"].ToString());
                _agente_end = dr["agente_end"].Equals(System.DBNull.Value) ? new DateTime() : Convert.ToDateTime(dr["agente_end"].ToString());
                _modo = dr["modo"].Equals(System.DBNull.Value) ? null : dr["modo"].ToString();
                _atendida = dr["atendida"].Equals(System.DBNull.Value) ? null : dr["atendida"].ToString();
                _record = dr["record"].Equals(System.DBNull.Value) ? null : dr["record"].ToString();
                _a_agente = dr["a_agente"].Equals(System.DBNull.Value) ? null : dr["a_agente"].ToString();
                _v_score = dr["v_score"].Equals(System.DBNull.Value) ? null : dr["v_score"].ToString();
                _conv = dr["conv"].Equals(System.DBNull.Value) ? null : dr["conv"].ToString();
                _dur_file = dr["dur_file"].Equals(System.DBNull.Value) ? null : dr["dur_file"].ToString();
                _tab_1 = dr["tab_1"].Equals(System.DBNull.Value) ? null : dr["tab_1"].ToString();
                _tab_2 = dr["tab_2"].Equals(System.DBNull.Value) ? null : dr["tab_2"].ToString();
                _h_agente = dr["h_agente"].Equals(System.DBNull.Value) ? null : dr["h_agente"].ToString();
                _d_uniqueid = dr["d_uniqueid"].Equals(System.DBNull.Value) ? null : dr["d_uniqueid"].ToString();
                _dialstatus = dr["dialstatus"].Equals(System.DBNull.Value) ? null : dr["dialstatus"].ToString();
                _dialinidate = dr["dialinidate"].Equals(System.DBNull.Value) ? new DateTime() : Convert.ToDateTime(dr["dialinidate"].ToString());

                acessoSQL.inserirDados(_channel, _ani, _agente, _extension, _dnis, _virtual_group, _outgoing_call, _date_start, _date_end, _acd_start, _agente_start, _agente_end, _modo, _atendida, _record, _a_agente, _v_score, _conv, _dur_file, _tab_1, _tab_2, _h_agente, _d_uniqueid, _dialstatus, _dialinidate);
            }
            Console.Write("Dados Inseridos com sucesso no SQLSERVER\n");
            Console.Write("Finalizando transferência...\n");
        }
    }
}
