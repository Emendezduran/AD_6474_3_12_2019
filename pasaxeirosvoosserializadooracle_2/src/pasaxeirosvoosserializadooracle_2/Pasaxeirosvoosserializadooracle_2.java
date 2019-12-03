/*
tablas:

- pasaxeiros

dni varchar2(5),
nome varchar2(15),
telf varchar2(10),
cidade varchar2(10),
nreservas integer


- voos

voo integer,
orixe varchar2(15),
destino varchar2(15),
prezo integer

- reservasfeitas

codr integer,
dni varchar2(5),
nome varchar2(15),
prezoreserva integer,
primary key(codr)


 */
package pasaxeirosvoosserializadooracle_2;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.ObjectInputStream;
import static java.rmi.server.ObjID.read;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

/**
 *
 * @author emendezduran_6474
 */
public class Pasaxeirosvoosserializadooracle_2 {

    public static Connection conexion = null;
    static File file = new File("/home/oracle/Desktop/compartido/pasaxeirosvoosserializadooracle_2/res/reservas");

    public static Connection getConexion() throws SQLException {
        String usuario = "hr";
        String password = "hr";
        String host = "localhost";
        String puerto = "1521";
        String sid = "orcl";
        String ulrjdbc = "jdbc:oracle:thin:" + usuario + "/" + password + "@" + host + ":" + puerto + ":" + sid;

        conexion = DriverManager.getConnection(ulrjdbc);
        return conexion;
    }

    public static void closeConexion() throws SQLException {
        conexion.close();
    }

    public static ArrayList<Reserva> leerSerializado(File file) {
        ArrayList<Reserva> reservasLeidas = new ArrayList<>();
        ObjectInputStream read;

        try {
            read = new ObjectInputStream(new BufferedInputStream(new FileInputStream(file)));
            while (true) {
                Object obj = read.readObject();
                if (obj == null) {
                    break;
                }
                reservasLeidas.add((Reserva) obj);
            }
            read.close();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return reservasLeidas;
    }

    public static int selectPrezoVoo(int voo) throws SQLException {
        String sql = "Select prezo from voos where voo='" + voo + "'";
        int prezo = 0;
        //conexion
        Connection conn = getConexion();
        //intermediario
        Statement statement = conn.createStatement();
        //resultados
        ResultSet rs = statement.executeQuery(sql);
        while (rs.next()) {
            prezo = rs.getInt("prezo");
        }
        return prezo;
    }

    public static Integer calcularPrezo(int precioIda, int precioVuelta) {
        int precioTotal = precioIda + precioVuelta;
        return precioTotal;
    }
    
    public static String selectNome(String dni) throws SQLException{
        String sql = "Select nome from pasaxeiros where dni='" + dni + "'";
        String nome = "";
        //conexion
        Connection conn = getConexion();
        //intermediario
        Statement statement = conn.createStatement();
        //resultados
        ResultSet rs = statement.executeQuery(sql);
        while (rs.next()) {
            nome = rs.getString("nome");
        }
        return nome;      
    }
    
    public static int selectNReservas(String dni) throws SQLException {
        String sql = "Select nreservas from pasaxeiros where dni='" + dni + "'";
        int nreservas = 0;
        //conexion
        Connection conn = getConexion();
        //intermediario
        Statement statement = conn.createStatement();
        //resultados
        ResultSet rs = statement.executeQuery(sql);
        while (rs.next()) {
            nreservas = rs.getInt("nreservas");
        }
        return nreservas;
    }
    
    public static int insertarReserva(Reserva res, String nome, int prezo) throws SQLException{
        String sql = "insert into reservasfeitas values (?,?,?,?)";
        //conexion
        Connection conn = getConexion();
        //intermediario
        PreparedStatement ps = conn.prepareStatement(sql);
        ps.setInt(1, res.getCodr());
        ps.setString(2, res.getDni());
        ps.setString(3, nome);
        ps.setInt(4, prezo);
        int rows = ps.executeUpdate();
        return rows;
    }
    
    public static int updatePasaxeiro(int nreservas, String dni) throws SQLException{
        String sql = "update pasaxeiros set nreservas = ? where dni= ?";
        //conexion
        Connection conn = getConexion();
         //intermediario
        PreparedStatement ps = conn.prepareStatement(sql);
        ps.setInt(1, nreservas);
        ps.setString(2, dni);
        int rows = ps.executeUpdate();
        return rows;
    }

    public static void main(String[] args) throws SQLException {
        
        //Lecuta del fichero serializado
        for (Reserva res : leerSerializado(file)) {
            System.out.println(res.toString());
            
            //Consulta de prezo ida/vuelta y calculo del precio TOTAL
            int prezoTotal = calcularPrezo(selectPrezoVoo(res.getIdvooida()), selectPrezoVoo(res.getIdvoovolta()));
            System.out.println("Precio TOTAL: " + prezoTotal);
            
            //consulta de nreservas actuales y se le suma 1
            int re = (selectNReservas(res.getDni()) + 1);
            
            //Update nreservas en pasaxeiros
            System.out.println(updatePasaxeiro(re, res.getDni()) + " Update realizada correctamente.");
            
            //Insert reservasfeitas
            System.out.println(insertarReserva(res, selectNome(res.getDni()), prezoTotal)+ " Insercion realizada correctamente.");
            
            System.out.println("**********************************************************************\n");
        }

        closeConexion();
    }

}
