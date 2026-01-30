
package com.example;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.ResultSet;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api")
public class ConcatController {

    @Value("${spring.datasource.url}")
    private String dbUrl;

    @Value("${spring.datasource.username}")
    private String dbUsername;

    @Value("${spring.datasource.password}")
    private String dbPassword;

    @Value("${spring.datasource.driver-class-name}")
    private String dbDriver;

    @PostMapping(value = "/concat", consumes = "application/json", produces = "application/json")
    public ResponseEntity<Map<String, Object>> concatFields(@RequestBody InputWrapper input) {

        // Read X-RqUID directly from input Header.XRqUID
        String rqUID = (input.getHeader() != null && input.getHeader().getXRqUID() != null)
                ? input.getHeader().getXRqUID()
                : "";

        String tipoIdentificacion = (input.getBody() != null) ? input.getBody().getTipoIdentificacion() : null;
        String numeroIdentificacion = (input.getBody() != null) ? input.getBody().getNumeroIdentificacion() : null;
        String tipoMediodePago = (input.getBody() != null) ? input.getBody().getTipoMediodePago() : null;
        String numeroMediodePago = (input.getBody() != null) ? input.getBody().getNumeroMediodePago() : null;

        // ===== Basic validation with logging to CM_UNIQUEKEY_LOG =====
        if (tipoIdentificacion == null || tipoIdentificacion.trim().isEmpty()) {
            // Log 400 validation error (no concatenated key yet)
            logValidationError(rqUID, tipoIdentificacion, numeroIdentificacion, tipoMediodePago, numeroMediodePago,
                               "N/A", 400, "tipoIdentificacion is required");
            return buildErrorResponse(rqUID, HttpStatus.BAD_REQUEST, "tipoIdentificacion is required");
        }

        if (numeroIdentificacion == null || numeroIdentificacion.trim().isEmpty()) {
            logValidationError(rqUID, tipoIdentificacion, numeroIdentificacion, tipoMediodePago, numeroMediodePago,
                               "N/A", 400, "numeroIdentificacion is required");
            return buildErrorResponse(rqUID, HttpStatus.BAD_REQUEST, "numeroIdentificacion is required");
        }

        Connection conn = null;
        PreparedStatement psSelect = null;
        PreparedStatement psUpdate = null;
        PreparedStatement psInsert = null;
        PreparedStatement psLog = null;
        ResultSet rs = null;

        String token = null;            // padded token ("0001", "0002", ...)
        String concatenatedKey = null;  // "@BPOP" + numeroIdentificacion + token

        try {
            // Load driver & open connection
            Class.forName(dbDriver);
            conn = DriverManager.getConnection(dbUrl, dbUsername, dbPassword);

            // 1) Read unique_id for given id_type_cd + id_value
            String selectSql = "SELECT unique_id FROM cm_uniquekey WHERE id_type_cd = ? AND id_value = ?";
            psSelect = conn.prepareStatement(selectSql);
            psSelect.setString(1, tipoIdentificacion);
            psSelect.setString(2, numeroIdentificacion);
            rs = psSelect.executeQuery();

            if (rs.next()) {
                // Existing record: increment numeric value of stored padded token
                String currentToken = rs.getString("unique_id"); // e.g., "0007"
                int currentTokenInt;
                try {
                    currentTokenInt = (currentToken != null && !currentToken.trim().isEmpty())
                            ? Integer.parseInt(currentToken) // "0007" -> 7
                            : 0;
                } catch (NumberFormatException e) {
                    currentTokenInt = 0;
                }

                int newTokenInt = currentTokenInt + 1;
                token = String.format("%04d", newTokenInt); // store padded "0008"

                String updateSql = "UPDATE cm_uniquekey SET unique_id = ?, XRQUID = ? WHERE id_type_cd = ? AND id_value = ?";
                psUpdate = conn.prepareStatement(updateSql);
                psUpdate.setString(1, token);               // padded
                psUpdate.setString(2, rqUID);
                psUpdate.setString(3, tipoIdentificacion);
                psUpdate.setString(4, numeroIdentificacion);
                psUpdate.executeUpdate();

            } else {
                // New record: start with padded "0001"
                token = "0001";
                String insertSql = "INSERT INTO cm_uniquekey (id_type_cd, id_value, unique_id, XRQUID) VALUES (?, ?, ?, ?)";
                psInsert = conn.prepareStatement(insertSql);
                psInsert.setString(1, tipoIdentificacion);
                psInsert.setString(2, numeroIdentificacion);
                psInsert.setString(3, token);    // padded
                psInsert.setString(4, rqUID);
                psInsert.executeUpdate();
            }

            // 2) Build the concatenated key with padded token
            concatenatedKey = "@BPOP" + numeroIdentificacion + token;

            // 3) Log success to CM_UNIQUEKEY_LOG (uppercase columns per DDL)
            String logSql =
                    "INSERT INTO CM_UNIQUEKEY_LOG " +
                    "(FECHA, XRQUID, TIPOIDENTIFICACION, NUMEROIDENTIFICACION, " +
                    " TIPOMEDIODEPAGO, NUMEROMEDIODEPAGO, LLAVESGENERADAS, STATUS_CD, MESSAGE_ERR) " +
                    "VALUES (SYSTIMESTAMP, ?, ?, ?, ?, ?, ?, ?, ?)";

            psLog = conn.prepareStatement(logSql);
            psLog.setString(1, rqUID);                  // XRQUID
            psLog.setString(2, tipoIdentificacion);     // TIPOIDENTIFICACION
            psLog.setString(3, numeroIdentificacion);   // NUMEROIDENTIFICACION
            psLog.setString(4, tipoMediodePago);        // TIPOMEDIODEPAGO
            psLog.setString(5, numeroMediodePago);      // NUMEROMEDIODEPAGO
            psLog.setString(6, concatenatedKey);        // LLAVESGENERADAS
            psLog.setInt(7, 200);                       // STATUS_CD
            psLog.setString(8, "Success");              // MESSAGE_ERR
            psLog.executeUpdate();

            // 4) Build response body (SUCCESS): msgHdrResponse + body
            Map<String, Object> msgHdrResponse = new LinkedHashMap<String, Object>();
            msgHdrResponse.put("X-RqUID", rqUID);               // echo input
            // Per your sample, X-AprovalId equals X-RqUID. Change to concatenatedKey if you want the approval id to be the key.
            msgHdrResponse.put("X-AprovalId", rqUID);
            msgHdrResponse.put("mensajeRespuesta", "Se generó la llave con exito");
            msgHdrResponse.put("codigoRespuesta", 200);

            Map<String, Object> bodyNode = new LinkedHashMap<String, Object>();
            List<String> llavesGeneradas = new ArrayList<String>();
            llavesGeneradas.add(concatenatedKey);               // "@BPOP<numeroIdentificacion><0001>"
            bodyNode.put("llavesGeneradas", llavesGeneradas);

            Map<String, Object> responseBody = new LinkedHashMap<String, Object>();
            responseBody.put("msgHdrResponse", msgHdrResponse);
            responseBody.put("body", bodyNode);                 // body **visible only on success**

            // Optional HTTP headers mirroring msgHdrResponse
            HttpHeaders headers = new HttpHeaders();
            headers.add("X-RqUID", rqUID);
            headers.add("X-AprovalId", rqUID); // or concatenatedKey

            return new ResponseEntity<Map<String, Object>>(responseBody, headers, HttpStatus.OK);

        } catch (Exception e) {
            // Attempt error logging to CM_UNIQUEKEY_LOG (500)
            logValidationError(rqUID, tipoIdentificacion, numeroIdentificacion, tipoMediodePago, numeroMediodePago,
                               (concatenatedKey != null ? concatenatedKey : "N/A"),
                               500, e.getMessage());

            // ERROR response: **NO 'body' key**
            return buildErrorResponse(rqUID, HttpStatus.INTERNAL_SERVER_ERROR, e.getMessage());

        } finally {
            // Close resources
            try { if (rs != null) rs.close(); } catch (SQLException ignored) {}
            try { if (psSelect != null) psSelect.close(); } catch (SQLException ignored) {}
            try { if (psUpdate != null) psUpdate.close(); } catch (SQLException ignored) {}
            try { if (psInsert != null) psInsert.close(); } catch (SQLException ignored) {}
            try { if (psLog != null) psLog.close(); } catch (SQLException ignored) {}
            try { if (conn != null) conn.close(); } catch (SQLException ignored) {}
        }
    }

    /**
     * Logs validation (or processing) errors to CM_UNIQUEKEY_LOG with STATUS_CD and MESSAGE_ERR.
     * Uses short-lived connection to avoid interfering with main flow.
     */
    private void logValidationError(String rqUID,
                                    String tipoIdentificacion,
                                    String numeroIdentificacion,
                                    String tipoMediodePago,
                                    String numeroMediodePago,
                                    String llavesGeneradasOrNA,
                                    int statusCode,
                                    String message) {
        Connection conn = null;
        PreparedStatement psLog = null;
        try {
            Class.forName(dbDriver);
            conn = DriverManager.getConnection(dbUrl, dbUsername, dbPassword);

            String logSql =
                    "INSERT INTO CM_UNIQUEKEY_LOG " +
                    "(FECHA, XRQUID, TIPOIDENTIFICACION, NUMEROIDENTIFICACION, " +
                    " TIPOMEDIODEPAGO, NUMEROMEDIODEPAGO, LLAVESGENERADAS, STATUS_CD, MESSAGE_ERR) " +
                    "VALUES (SYSTIMESTAMP, ?, ?, ?, ?, ?, ?, ?, ?)";

            psLog = conn.prepareStatement(logSql);
            psLog.setString(1, rqUID);                                // XRQUID (echo input)
            psLog.setString(2, tipoIdentificacion);                   // can be null
            psLog.setString(3, numeroIdentificacion);                 // can be null
            psLog.setString(4, tipoMediodePago);                      // can be null
            psLog.setString(5, numeroMediodePago);                    // can be null
            psLog.setString(6, (llavesGeneradasOrNA != null) ? llavesGeneradasOrNA : "N/A"); // "N/A" when no key
            psLog.setInt(7, statusCode);
            psLog.setString(8, message);
            psLog.executeUpdate();
        } catch (Exception ignore) {
            // Swallow logging errors; do not block response
        } finally {
            try { if (psLog != null) psLog.close(); } catch (SQLException ignored) {}
            try { if (conn != null) conn.close(); } catch (SQLException ignored) {}
        }
    }

    /**
     * Builds an error response with only 'msgHdrResponse' (no 'body').
     */
    private ResponseEntity<Map<String, Object>> buildErrorResponse(String rqUID, HttpStatus status, String message) {
        Map<String, Object> msgHdrResponse = new LinkedHashMap<String, Object>();
        msgHdrResponse.put("X-RqUID", rqUID);   // echo input value
        msgHdrResponse.put("X-AprovalId", "");  // empty on error
        msgHdrResponse.put("mensajeRespuesta", message);
        msgHdrResponse.put("codigoRespuesta", status.value());

        Map<String, Object> responseBody = new LinkedHashMap<String, Object>();
        responseBody.put("msgHdrResponse", msgHdrResponse);
        // **No 'body' key on error**

        HttpHeaders headers = new HttpHeaders();
        headers.add("X-RqUID", rqUID);

        return new ResponseEntity<Map<String, Object>>(responseBody, headers, status);
    }
}
