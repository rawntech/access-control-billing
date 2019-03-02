/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package mssccesstopostgres_prome;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.commons.lang.time.DateUtils;
/**
 *
 * @author NOSEABD
 */
public class cardupdatefrompos {
    
    public static void main(String[] args) throws Exception {
       Connection cp = null;
       Statement stmtp = null;
       Connection ca = null;
       Statement stmta = null;
       String maxTIme = "2019-02-01 00:00:00.0000";
       String device = null;
       int hunits = 0;
      //  for (int devicesl =1; devicesl < 200; devicesl++) {
       try {
       Class.forName("org.postgresql.Driver");
         cp = DriverManager
            .getConnection("jdbc:postgresql://localhost:5433/pos2019",
            "postgres", "syspassrawn");
         cp.setAutoCommit(false);
         System.out.println("Opened  Postgress database successfully");
         stmtp = cp.createStatement();
         ResultSet rs = stmtp.executeQuery( "select maxtime, tlu.units , device  from (select  max(r.datenew) maxtime, p.reference device  from receipts r, tickets t, ticketlines tl, products p\n" +
"where r.id=t.id\n" +
"and t.id=tl.ticket\n" +
"and tl.product=p.id group by p.reference "
        + ") d, ticketlines tlu, receipts ru, products pu\n" +
"where d.maxtime=ru.datenew\n" +
"and ru.id=tlu.ticket\n" +
"and tlu.product=pu.id\n" +
" and d.device=pu.reference " + 
" and ru.datenew + interval '1 hour' > now();" );
         while ( rs.next() ) {
         maxTIme = rs.getString("maxtime");
         device = rs.getString("device");
         hunits = rs.getInt("units");
         if( maxTIme == null)
         {
             maxTIme="2019-02-05 00:00:00.0000";
             
         }
         System.out.println(device + " Max Time " +maxTIme);
           
       
       
       try {
         Class.forName("net.ucanaccess.jdbc.UcanaccessDriver");
         ca = DriverManager
             .getConnection("jdbc:ucanaccess://C:/AccessControl/iCCard3000.mdb",
            "", "168168");
         ca.setAutoCommit(false);
         System.out.println("Opened Access database successfully");
        stmta = ca.createStatement();
           
           try (  
                   ResultSet rsa = stmta.executeQuery( "SELECT t_b_Consumer.f_ConsumerID, t_b_Consumer.f_CardNO, t_b_Consumer.f_EndYMD\n" +
"FROM t_b_Consumer where t_b_Consumer.f_ConsumerID ="+ device+";")) {
                   
 
               while ( rsa.next() ) {
                   String userNo = rsa.getString("f_ConsumerID");
                   String checkTime = rsa.getString("f_EndYMD");
                   // string to date
                   DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                   
                   // Convert from String to Date
                   Date dateMaxTime = df.parse(maxTIme);
                   
                  // if(dateCheckTime.before(dateMaxTime)){  //dcheck max time issues
                    System.out.println("Before Max Time" + maxTIme);
                       // DateUtils.addHours(dateMaxTime, 1);
                       Date addHours = DateUtils.addHours(dateMaxTime, hunits);
                       
                       System.out.println("After Max Time" + addHours);
                       maxTIme = df.format(addHours); 
                       
                       
                       System.out.println("Opened  Postgress database successfully");
                       System.out.println("User " + userNo +" Time "+ checkTime);

                       String sql = "update  t_b_Consumer  set  t_b_Consumer.f_EndYMD = '" + maxTIme
                              + "' where f_ConsumerID =" + device + ";";
                      System.out.println(sql);
                      stmta.executeUpdate(sql);
                       ca.commit();
          
                       System.out.println("User End " + userNo +" Time "+ maxTIme);
                  // } check max time issues
               } }

            }
       
        catch (Exception ex) {
            ex.printStackTrace();
                }
    }
   }
        catch (Exception ex) {
            ex.printStackTrace();
                }finally {

        try {
            if (stmtp != null) {
                stmtp.close();
            }
            if (cp != null) {
                cp.close();
            }
             if (stmta != null) {
                stmta.close();
            }
             if (ca != null) {
                ca.close();
            }
        }
catch (Exception ex) {
            ex.printStackTrace();
                }

    }
    }
}
