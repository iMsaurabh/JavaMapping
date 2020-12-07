package com.sap.pi.tongwei.xml2json;

import java.io.InputStream;
import java.io.OutputStream;

import org.json.JSONArray;
import org.json.JSONObject;
import org.json.XML;

import com.sap.aii.mapping.api.AbstractTransformation;
import com.sap.aii.mapping.api.StreamTransformationException;
import com.sap.aii.mapping.api.TransformationInput;
import com.sap.aii.mapping.api.TransformationOutput;

public class XML2JSONConversion extends AbstractTransformation {

	@Override
	public void transform(TransformationInput in, TransformationOutput out) throws StreamTransformationException {
		// TODO Auto-generated method stub
		this.execute(in.getInputPayload().getInputStream(), out.getOutputPayload().getOutputStream());
	}

	// Create your custom Method for XMLtoJSON Conversion
	private void execute(InputStream inputStream, OutputStream outputStream) {
		try {
			// If data is present in InputStream, it is stored in byte object
			byte[] buf = new byte[inputStream.available()];

			// Inputstream reads the data in byte format
			inputStream.read(buf);

			// A debug message is added to display the input XML
			if (getTrace() != null) {
				getTrace().addDebugMessage("Input XML:\n" + new String(buf, "utf-8") + "\n ------");
			} else { // This section is added for normal JAVA Program run
				System.out.println(new String(buf, "utf-8"));
			}

			// Convert XML to JSON
			JSONObject json = XML.toJSONObject(new String(buf, "utf-8"));

			// Creating place holder JSON Object and Arrays
			JSONObject AnonymousJSONObject = new JSONObject();
			JSONObject Request = new JSONObject();
			JSONObject Data = new JSONObject();
			JSONArray P_TAB_PARA = new JSONArray();

			// Strip outer element
			JSONObject root_obj = json.getJSONObject("ns0:TongweiContractPARRequest");

			root_obj.remove("xmlns:ns0");
			// System.out.println(Request.toString(4));

			JSONObject request_obj = root_obj.getJSONObject("Request");
			// System.out.println(Request.toString(4));

			// Head JSONObject
			JSONObject head_obj = request_obj.getJSONObject("Head");
			// System.out.println(Head.toString(4));

			// Data JSONObject
			JSONObject data_obj = request_obj.getJSONObject("Data");
			// System.out.println(Data.toString(4));

			// P_TAB_PARA JSONObject
			JSONObject p_tab_para_obj = data_obj.getJSONObject("P_TAB_PARA");
			// System.out.println(P_TAB_PARA.toString(4));

			// P_TAB_PARA_LV0 JSONObject
			JSONArray p_tab_para_lv0_arr = p_tab_para_obj.getJSONArray("P_TAB_PARA_LV0");
			// System.out.println(p_tab_para_lv0_arr.toString(4));

			// Iterating over P_TAB_PARA_LV0 to fetch all pairs
			for (int j = 0; j < p_tab_para_lv0_arr.length(); j++) {
				JSONObject keyValue_obj = p_tab_para_lv0_arr.getJSONObject(j);
				// placing each pair directly in P_TAB_PARA
				P_TAB_PARA.put(keyValue_obj);

			}
			// System.out.println(P_TAB_PARA.toString(4));

			// P_TAB_PARA_LV1 JSONObject
			JSONArray p_tab_para_lv1_arr = p_tab_para_obj.getJSONArray("P_TAB_PARA_LV1");
			// System.out.println(p_tab_para_lv1_arr.toString(4));

			// P_TAB_PARA_LV1 JSONObject
			JSONArray P_TAB_PARA_LV1 = new JSONArray();
			for (int j = 0; j < p_tab_para_lv1_arr.length(); j++) {
				JSONObject keyValue_obj = p_tab_para_lv1_arr.getJSONObject(j);
				JSONArray p_tab_para_lv2_arr = keyValue_obj.getJSONArray("P_TAB_PARA_LV2");
				// placing each pair directly in P_TAB_PARA
				P_TAB_PARA_LV1.put(p_tab_para_lv2_arr);
			}
			P_TAB_PARA.put(P_TAB_PARA_LV1);
			// System.out.println(P_TAB_PARA.toString(4));
			Data.put("P_TAB_PARA", P_TAB_PARA);
			// System.out.println(Data.toString(4));

			Request.put("Data", Data);
			Request.put("Head", head_obj);
			// System.out.println(Request.toString(4));
			AnonymousJSONObject.put("Request", Request);

			// outputting the JSON
			byte[] bytes = AnonymousJSONObject.toString().getBytes("UTF-8");

			// Write output bytes to the output stream
			outputStream.write(bytes);

		} catch (Exception e) {
			e.printStackTrace();
		}

	}
}
