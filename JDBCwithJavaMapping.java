package com.bunge.delta.dbudpate;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.OutputStream;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.w3c.dom.Attr;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.sap.aii.mapping.api.AbstractTransformation;
import com.sap.aii.mapping.api.DynamicConfiguration;
import com.sap.aii.mapping.api.DynamicConfigurationKey;
import com.sap.aii.mapping.api.StreamTransformationException;
import com.sap.aii.mapping.api.TransformationInput;
import com.sap.aii.mapping.api.TransformationOutput;
import com.sap.engine.interfaces.messaging.api.MessageDirection;
import com.sap.engine.interfaces.messaging.api.MessageKey;
import com.sap.engine.interfaces.messaging.api.PublicAPIAccessFactory;
import com.sap.engine.interfaces.messaging.api.auditlog.AuditAccess;
import com.sap.engine.interfaces.messaging.api.auditlog.AuditLogStatus;

public class InsertIntoDB extends AbstractTransformation {

	@Override
	public void transform(TransformationInput in, TransformationOutput out) throws StreamTransformationException {
		// Fetching runtime parameters from parameterization
		String connectionString = in.getInputParameters().getString("connectionString");
		String username = in.getInputParameters().getString("username");
		String password = in.getInputParameters().getString("password");
		String method = in.getInputParameters().getString("method");
		String tableName = in.getInputParameters().getString("tableName");

		// Setting up Audit log access
		MessageKey key = null;

		AuditAccess audit = null;

		final String DASH = "-";
		try {
			String msgID = in.getInputHeader().getMessageId();
			// Convert message ID to UUID format (in compliance to RFC 4122).
			// UUID format is used by Advanced Adapter Engine for identifiers of processed
			// messages.
			// For the sake of simplicity, conversion is done manually - alternatively,
			// specific libraries can be used for this.
			String uuidTimeLow = msgID.substring(0, 8);
			String uuidTimeMid = msgID.substring(8, 12);
			String uuidTimeHighAndVersion = msgID.substring(12, 16);
			String uuidClockSeqAndReserved = msgID.substring(16, 18);
			String uuidClockSeqLow = msgID.substring(18, 20);
			String uuidNode = msgID.substring(20, 32);
			String msgUUID = uuidTimeLow + DASH + uuidTimeMid + DASH + uuidTimeHighAndVersion + DASH
					+ uuidClockSeqAndReserved + uuidClockSeqLow + DASH + uuidNode;
			// Construct message key (com.sap.engine.interfaces.messaging.api.MessageKey)
			// for retrieved message ID and outbound message direction
			// (com.sap.engine.interfaces.messaging.api.MessageDirection).
			audit = PublicAPIAccessFactory.getPublicAPIAccess().getAuditAccess();

			key = new MessageKey(msgUUID, MessageDirection.OUTBOUND);
		} catch (Exception e) {
			// Exception handling logic.
			e.printStackTrace();
		}

		// Setting Dynamic parameter for insert_count
		DynamicConfiguration conf = in.getDynamicConfiguration();
		DynamicConfigurationKey key1 = DynamicConfigurationKey.create("strInsertedRecords", "insertedRecords");

		// Executing the mapping
		this.execute(in.getInputPayload().getInputStream(), out.getOutputPayload().getOutputStream(), connectionString,
				username, password, method, tableName, audit, key, conf, key1);
	}

	private void execute(InputStream inputStream, OutputStream outputStream, String connectionString, String username,
			String password, String method, String tableName, AuditAccess audit, MessageKey key,
			DynamicConfiguration conf, DynamicConfigurationKey key1) {

		try {
			// If data is present in InputStream, it is stored in byte object
			byte[] buf = new byte[inputStream.available()];

			// Inputstream reads the data in byte format
			inputStream.read(buf);

			// XML Parsing code here
			// Instantiate the Factory
			DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();

			// process XML securely, avoid attacks like XML External Entities (XXE)
			// (optional, but recommended)
			dbf.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);

			// parse XML file
			DocumentBuilder db = dbf.newDocumentBuilder();
			Document doc = db.parse(new ByteArrayInputStream(buf));

			// Normalization in DOM parsing (optional, but recommended)
			doc.getDocumentElement().normalize();

			// Fetching collection of access node structures
			NodeList records = doc.getElementsByTagName("access");
			
			NodeList firstRecord = records.item(0).getChildNodes();

			// Generate compiled prepared statement
			String sqlStatementPref = method + " INTO " + tableName + " (";
			String sqlStatementSuff = ") ";
			String sqlStatementValuesPref = "VALUES (";
			String sqlStatementValuesSuff = ")";
			String columnNames = "";
			String columnValues = "";
			
			for (int j = 0; j < firstRecord.getLength(); j++) {
				Node node = firstRecord.item(j);
				if (node.getNodeType() == Node.ELEMENT_NODE) {
					Element element = (Element) node;
					sqlStatementPref += element.getNodeName() + ", ";
					sqlStatementValuesPref += "?, ";
				}
			}

			String insert_record = sqlStatementPref + columnNames + sqlStatementSuff + sqlStatementValuesPref
					+ columnValues + sqlStatementValuesSuff;
			insert_record = insert_record.replace(", )", ")");

			// Setting up DB connection settings
			String url = connectionString;
			Connection conn = DriverManager.getConnection(url, username, password);
			try {
				// disabling auto commit
				conn.setAutoCommit(false);

				// Instantiate prepared statement
				PreparedStatement pstmt = conn.prepareStatement(insert_record);
				// Create Batch of statements for each record
				createBatches(records, pstmt);
				audit.addAuditLogEntry(key, AuditLogStatus.SUCCESS, "Batch Collection Finished.");
				audit.addAuditLogEntry(key, AuditLogStatus.SUCCESS, "Executing the batch !!");
				// Update status of count array for each batch
				int[] updateCounts = pstmt.executeBatch();

				audit.addAuditLogEntry(key, AuditLogStatus.SUCCESS, "Batch Execution finished successfully.");

				// Closing prepared statement once done for all records
				audit.addAuditLogEntry(key, AuditLogStatus.SUCCESS, "Closing the prepared Statement !!");
				pstmt.close();
				// Committing records to DB
				audit.addAuditLogEntry(key, AuditLogStatus.SUCCESS, "Committing the records to DB");
				conn.commit();
				// Closing the connection
				audit.addAuditLogEntry(key, AuditLogStatus.SUCCESS, "Closing the connection from Mapping");
				conn.close();
				audit.addAuditLogEntry(key, AuditLogStatus.SUCCESS, "Connection Closed.");
				audit.addAuditLogEntry(key, AuditLogStatus.SUCCESS,
						"Number of records inserted successfully: " + String.valueOf(updateCounts.length));
				// Pass the number of records as dynamic parameter
				conf.put(key1, String.valueOf(updateCounts.length));

			} catch (Exception e) {
				getTrace().addInfo(e.getMessage());
				// Got an exception, starting roll back
				audit.addAuditLogEntry(key, AuditLogStatus.ERROR, "Got an exception!");
				audit.addAuditLogEntry(key, AuditLogStatus.ERROR, "Rolling back data here...");
				try {
					if (conn != null) {
						conn.rollback();
						conn.close();
					}
				} catch (SQLException se) {
					getTrace().addInfo(se.getMessage());
					// Add exception to Audit Log
					audit.addAuditLogEntry(key, AuditLogStatus.ERROR, se.getMessage());
					conf.put(key1, se.getMessage());
				}
				getTrace().addWarning(e.getMessage());
				audit.addAuditLogEntry(key, AuditLogStatus.ERROR, e.getMessage());
				conf.put(key1, e.getMessage());
			}

		} catch (Exception e) {
			getTrace().addInfo(e.getMessage());
			audit.addAuditLogEntry(key, AuditLogStatus.ERROR, e.getMessage());
			// Publish exception to dynamic param InsertedRecords
			conf.put(key1, e.getMessage());
		}

		// Creating target document for output of java mapping
		DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
		DocumentBuilder builder;
		try {
			builder = factory.newDocumentBuilder();

			Document targetDoc = builder.newDocument();
			// Creating target root and namespace

			Element targetRoot = targetDoc.createElement("ns0:PosiDataCustomStatement");
			targetRoot.setAttribute("xmlns:ns0", "http://bunge.com/Delta");
			targetDoc.appendChild(targetRoot);
			// Creating target structure
			Element statement = targetDoc.createElement("Statement");
			Element SQL_QUERY = targetDoc.createElement("SQL_QUERY");
			Attr action = targetDoc.createAttribute("action");
			action.setValue("SQL_QUERY");
			Element access = targetDoc.createElement("access");
			access.setTextContent("SELECT user FROM Dual WHERE ROWNUM = 1");

			// Organizing the structure
			targetRoot.appendChild(statement);
			statement.appendChild(SQL_QUERY);
			SQL_QUERY.setAttributeNode(action);
			SQL_QUERY.appendChild(access);

			// Creating DOM Parsed target document for output stream
			DOMSource domSource = new DOMSource(targetDoc);
			StreamResult result = new StreamResult(outputStream);

			// Creating the output template to send final output
			TransformerFactory tf = TransformerFactory.newInstance();
			Transformer transformer = tf.newTransformer();
			transformer.transform(domSource, result);
		} catch (ParserConfigurationException | TransformerException e) {
			audit.addAuditLogEntry(key, AuditLogStatus.ERROR, e.getMessage());
			conf.put(key1, e.getMessage());
		}
	}

	private void createBatches(NodeList records, PreparedStatement pstmt) throws SQLException {
		// Index I counts number records to be inserted in DB
		for (int i = 0; i < records.getLength(); i++) {
			// Below statement selects new record
			NodeList recordColumns = records.item(i).getChildNodes();
			// index k keeps track of number of column for which value is being fetched
			int k = 1;
			// index j keep track of columns to be inserted
			for (int j = 0; j < recordColumns.getLength(); j++) {
				// Selecting new field for current record
				Node node = recordColumns.item(j);
				// checking node is an element
				if (node.getNodeType() == Node.ELEMENT_NODE) {
					Element element = (Element) node;
					// Fetching the value for current element/fieldname
					String keyValue = (((element.getTextContent().equalsIgnoreCase(""))
							|| (element.getTextContent().equalsIgnoreCase("null")) ? null
									: element.getTextContent()));
					// setting up prepared statement for each field/column
					pstmt.setString(k, keyValue);
					// incrementing to update index for new column
					k++;
				}
			}
			// Adding the statement to the batch
			pstmt.addBatch();
		}
		
	}
}
