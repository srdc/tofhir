package io.tofhir.engine;

import java.nio.ByteBuffer;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;

public class DBTest {
    private static final String url = "jdbc:postgresql://localhost:5432/dt4h_icrc_cdm";
    private static final String user = "postgres";
    private static final String password = "password";

    public static void main(String[] args) throws SQLException {

    }

    private static void omopAnalysis() throws SQLException {
        String snomed2snomedMappings = "select c.concept_code, c2.concept_code from concept_relationship cr" +
                " join concept c on c.concept_id = cr.concept_id_1" +
                " join concept c2 on c2.concept_id = cr.concept_id_2" +
                " where cr.relationship_id = 'Maps to' and c.vocabulary_id = c2.vocabulary_id and c.vocabulary_id = 'SNOMED' and c.concept_code != c2.concept_code";

        Connection c = null;
        Statement st = null;
        ResultSet rs = null;
        try {
            c = connect();
            st = c.createStatement();
            rs = st.executeQuery(snomed2snomedMappings);

            while(rs.next()) {
                String code1 = rs.getString(1);
                String code2 = rs.getString(1);
            }

        } finally {
            rs.close();
            st.close();
            c.close();
        }
    }

    private static void decodeICRCBinary() throws SQLException {
        String query = "SELECT c_data FROM ecg.mortara_channel_data WHERE c_name = 'I'";

        Connection c = null;
        Statement st = null;
        ResultSet rs = null;
        try {
            c = connect();
            st = c.createStatement();
            rs = st.executeQuery(query);

            // iterate through the java resultset
            while (rs.next()) {
                String bin = rs.getString(1);
                System.out.println(bin);
                byte[] encoded = Base64.getEncoder().encode(bin.getBytes());
                List<Double> decodedNumbers = new ArrayList<>();
                for (int i = 0; i < encoded.length; i += 2) {
                    ByteBuffer bf = ByteBuffer.wrap(Arrays.copyOfRange(encoded, i, i + 2)).order(java.nio.ByteOrder.LITTLE_ENDIAN);
                    double decoded = bf.getShort() ;
                    System.out.println(decoded);
                    decodedNumbers.add(decoded);
                }
                System.out.println("Finished one item");
                break;
            }
        } finally {
            rs.close();
            st.close();
            c.close();
        }
    }

    private static Connection connect() {
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(url, user, password);
            System.out.println("Connected to the PostgreSQL server successfully.");
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }

        return conn;
    }
}

