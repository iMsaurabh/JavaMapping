package com.sap.pi.javamapping;

import java.io.InputStream;
import java.io.OutputStream;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.sap.aii.mapping.api.AbstractTransformation;
import com.sap.aii.mapping.api.StreamTransformationException;
import com.sap.aii.mapping.api.TransformationInput;
import com.sap.aii.mapping.api.TransformationOutput;
import com.sap.aii.mapping.lookup.Channel;
import com.sap.aii.mapping.lookup.LookupService;
import com.sap.aii.mapping.lookup.Payload;
import com.sap.aii.mapping.lookup.SystemAccessor;

public class TestJavaMapping extends AbstractTransformation {

	@Override
	public void transform(TransformationInput in, TransformationOutput out) throws StreamTransformationException {
		// TODO Auto-generated method stub
		String inParam = in.getInputParameters().getString("DynamicParam");
		getTrace().addInfo("Input Parameter: " + inParam);
		this.execute(in.getInputPayload().getInputStream(), out.getOutputPayload().getOutputStream(), inParam);
	}

	private void execute(InputStream inputStream, OutputStream outputStream, String inParam) {
		try {
			String status = "";
			// generate the input xml for rest look up
			// String loginxml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
			// "<zipcode>10001</zipcode>";
			// perform the rest look up
			Channel channel = LookupService.getChannel("BC_API", "CC_ReceiverGetToken");
			SystemAccessor accessor = null;
			accessor = LookupService.getSystemAccessor(channel);
			// InputStream loginPayloadStream = new
			// ByteArrayInputStream(loginxml.getBytes());
			Payload payload = LookupService.getXmlPayload(inputStream);
			Payload RESTOutPayload = null;
			// Creating output payload after REST Call
			RESTOutPayload = accessor.call(payload);

			// Creating stream from output payload to parse
			InputStream inp = RESTOutPayload.getContent();
			DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
			DocumentBuilder builder = factory.newDocumentBuilder();
			Document document = builder.parse(inp);
			NodeList stats = document.getElementsByTagName("token");
			Node node = stats.item(0);
			if (node != null) {
				node = node.getFirstChild();
				if (node != null) {
					status = node.getNodeValue();
				}
			}
			// Creating target document for output of java mapping
			Document targetDoc = builder.newDocument();

			// Creating target root and namespace
			Element targetRoot = (Element) targetDoc.createElement("ns0:MT_BJ_Response");
			targetRoot.setAttribute("xmlns:ns0", "http://bunge.com/java");

			// Creating target structure
			Element token = (Element) targetDoc.createElement("token");
			token.setTextContent(status);
			Element customParam = (Element) targetDoc.createElement("customParam");
			customParam.setTextContent(inParam);

			targetRoot.appendChild(token);
			targetRoot.appendChild(customParam);
			targetDoc.appendChild(targetRoot);

			// Creating DOM Parsed target document for output stream
			DOMSource domSource = new DOMSource(targetDoc);
			StreamResult result = new StreamResult(outputStream);

			// Creating the output template to send final output
			TransformerFactory tf = TransformerFactory.newInstance();
			Transformer transformer = tf.newTransformer();
			transformer.transform(domSource, result);
		} catch (Exception e) {
			e.printStackTrace();
		}
	} // end of execute
}
