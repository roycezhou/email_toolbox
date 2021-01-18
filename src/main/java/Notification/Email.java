package Notification;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.ParseException;
//import com.google.*;
//import com.google.protobuf.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.*;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.TableName;

import org.apache.hadoop.security.*;

import javax.imageio.ImageIO;
import javax.mail.*;
import javax.mail.internet.*;

public class Email
{
	
	public static synchronized Configuration login( Configuration conf, String kerberosPrincipal, String kerberosKeytab) throws IOException {
		//conf.set(USER_KEYTAB_KEY, kerberosKeytab );
		//conf.set(USER_PRINCIPAL_KEY, kerberosPrincipal );
		UserGroupInformation.setConfiguration( conf );
		UserGroupInformation.loginUserFromKeytab(kerberosPrincipal, kerberosKeytab );
		return conf;
	}
	
	public static void main(String [] args) throws ParseException, IOException {
		main_1(args, "");
		main_1(args, "B");
	}

	public static void main_1(String [] args, String side) throws ParseException, IOException {
		String namespace = args[6];
		double offset = Double.parseDouble(args[12]);
		Configuration conf = HBaseConfiguration.create();
		try {
            Connection connx = ConnectionFactory.createConnection(conf);
            String xHTName = namespace + ":xxx";
            Table table = connx.getTable(TableName.valueOf(xHTName.getBytes()));

			ArrayList <String> L= new ArrayList<String>();
			L=RetrieveRowKeys(args[0], args[1], args[4], namespace, side);
			
			String input1 = args[3].trim(), input2 = args[5].trim(), input3 = args[13].trim();
			String [] arr1 = null,  arr2 = null,  arr3 = null;
			if (input1.length() > 0)
				arr1 = input1.split(",");
			if (input2.length() > 0)
				arr2 = input2.split(",");
            if (input3.length() > 0)
                arr3 = input3.split(",");
		
			// Sender's email ID needs to be mentioned
			String from = args[2];
		
			// Assuming you are sending email from localhost
			String host = "localhost";
		
			// Get system properties
			Properties properties = System.getProperties();
		
			// Setup mail server
			properties.setProperty("mail.smtp.host", host);
		
			// Get the default Session object.
			Session session = Session.getDefaultInstance(properties);
	   
			try{
				ArrayList <String> tempFiles = new ArrayList <String> ();

                Connection connWS = ConnectionFactory.createConnection(conf);
                String yScribeHTName = namespace + ":xxx";
                Table wT = connWS.getTable(TableName.valueOf(yScribeHTName.getBytes()));

				for (int i=0; i<L.size();i++) {
					Filter filter = new PrefixFilter(Bytes.toBytes(L.get(i)));
					Get gg = new Get(Bytes.toBytes(L.get(i)));
					Result rr = table.get(gg);
					String DID = "";
					if (rr.getValue(Bytes.toBytes("data"), Bytes.toBytes("DeviceID")) != null)
						DID = Bytes.toString(rr.getValue(Bytes.toBytes("data"), Bytes.toBytes("DeviceID")));
					// Create a default MimeMessage object.
					MimeMessage message = new MimeMessage(session);
		
					// Set From: header field of the header.
					message.setFrom(new InternetAddress(from));
		
					// Set To: header field of the header.
					// Set Subject: header field
					boolean goodCatch = true;
					String[] element = L.get(i).split("_");
					Get g = new Get(Bytes.toBytes(element[1]+'.'+element[2]));
					Result r = wT.get(g);
		    		String Current_Date = element[0];
		    		String Current_zid=element[1];
		    		String Current_yScribe=element[2];
					element[2] = Bytes.toString(r.getValue(Bytes.toBytes("basic_info"), Bytes.toBytes("y_id")));
					for (int k = 4; k < element.length; k ++)
						element[3] += "_" + element[k];
		    		String Current_Traveler=element[3];
					g = new Get(Bytes.toBytes(L.get(i)));
					r = table.get(g);
					int xNo = Integer.parseInt(Bytes.toString(r.getValue(Bytes.toBytes("data"), Bytes.toBytes(side + "NO_xES"))));
					
					if (element[3].charAt(0) == 'Z')
						goodCatch = false;
					
					String tool = Bytes.toString(r.getValue(Bytes.toBytes("data"), Bytes.toBytes("InspectionStationID")));
					System.out.println("tool id: " + tool);
					if (tool != null && (tool.contains("RMACA12A00") || tool.contains("RMACA13A00") || 
							tool.contains("RMAC7AA200") || tool.contains("RMAC7AA300")))
						goodCatch = false;
					
					int j;
					for (j = 1; j <= xNo; j ++)
						if (r.getValue(Bytes.toBytes("data"), Bytes.toBytes(side + "Final_ToolList" + j)) != null 
							&& Bytes.toString(r.getValue(Bytes.toBytes("data"), Bytes.toBytes(side + "Final_ToolList" + j))) != ""
							&& !Bytes.toString(r.getValue(Bytes.toBytes("data"), Bytes.toBytes(side + "Final_ToolList" + j))).contains("Repeated"))
							break;
					if (j > xNo)
						goodCatch = false;
		
					Multipart multipart = new MimeMultipart();
				    String rptfilename, imgfilename, temp = null;
				      
				    //Retrive last scan image
				    Result r1 = Row_Retrieve(L.get(i), namespace);
				    if(column_exists(r1,"LastScan_RowKey") && !Bytes.toString(r1.getValue(Bytes.toBytes("data"),Bytes.toBytes("LastScan_RowKey"))).equals("")) {
				    	r1 = Row_Retrieve(Bytes.toString(r1.getValue(Bytes.toBytes("data"),Bytes.toBytes("LastScan_RowKey"))), namespace);
				    	if (r1 != null){ // && r1.getValue(Bytes.toBytes("data"), Bytes.toBytes("RawImage")) != null) {
				    		//FileSystem fs = FileSystem.get(conf);
				    		imgfilename = Bytes.toString(r1.getRow()).replace(' ', '_').replace(':', '_').replace('.', '_').toString() + ".jpg";
				    		temp = "/tmp/" + imgfilename;
				    		//String rawimagepath = Bytes.toString(r1.getValue(Bytes.toBytes("data"), Bytes.toBytes("imagepath")));
				    		yDefectProfile wdp = new yDefectProfile();
				    		String deviceID = Bytes.toString(r1.getValue(Bytes.toBytes("data"), Bytes.toBytes("DeviceID")));

                            Connection connDevice = ConnectionFactory.createConnection(conf);
                            String deviceHTName = namespace + ":" + (args[11] == "1" ? "alldevice" : deviceID);
                            Table devicetable = connDevice.getTable(TableName.valueOf(deviceHTName.getBytes()));

				    		Get get = new Get(Bytes.toBytes(Bytes.toString(r1.getRow())));
				    		Result lastScan = devicetable.get(get);
				    		if (lastScan.getValue(Bytes.toBytes("y_info"), Bytes.toBytes("DeviceID")) != null)
				    			wdp.deviceID = Bytes.toString(lastScan.getValue(Bytes.toBytes("y_info"), Bytes.toBytes("DeviceID")));
				    		if (lastScan.getValue(Bytes.toBytes("y_info"), Bytes.toBytes("InspectionStationID")) != null)
				    			wdp.inspToolID = Bytes.toString(lastScan.getValue(Bytes.toBytes("y_info"), Bytes.toBytes("InspectionStationID")));
				    		if (lastScan.getValue(Bytes.toBytes("y_info"), Bytes.toBytes("OrientationMarkLocation")) != null)
				    			wdp.orientation = Bytes.toString(lastScan.getValue(Bytes.toBytes("y_info"), Bytes.toBytes("OrientationMarkLocation")));
				    		if (lastScan.getValue(Bytes.toBytes("y_info"), Bytes.toBytes("DiePitchX")) != null)
				    			wdp.diePitchX = Double.parseDouble(Bytes.toString(lastScan.getValue(Bytes.toBytes("y_info"), Bytes.toBytes("DiePitchX"))));
				    		if (lastScan.getValue(Bytes.toBytes("y_info"), Bytes.toBytes("DiePitchY")) != null)
				    			wdp.diePitchY = Double.parseDouble(Bytes.toString(lastScan.getValue(Bytes.toBytes("y_info"), Bytes.toBytes("DiePitchY"))));
				    		if (lastScan.getValue(Bytes.toBytes("y_info"), Bytes.toBytes("DieOriginX")) != null)
				    			wdp.dieOriginX = Double.parseDouble(Bytes.toString(lastScan.getValue(Bytes.toBytes("y_info"), Bytes.toBytes("DieOriginX"))));
				    		if (lastScan.getValue(Bytes.toBytes("y_info"), Bytes.toBytes("DieOriginY")) != null)
				    			wdp.dieOriginY = Double.parseDouble(Bytes.toString(lastScan.getValue(Bytes.toBytes("y_info"), Bytes.toBytes("DieOriginY"))));
				    		if (lastScan.getValue(Bytes.toBytes("y_info"), Bytes.toBytes("SampleCenterLocationX")) != null)
				    			wdp.dieSampleOriginX = Double.parseDouble(Bytes.toString(lastScan.getValue(Bytes.toBytes("y_info"), Bytes.toBytes("SampleCenterLocationX"))));
				    		if (lastScan.getValue(Bytes.toBytes("y_info"), Bytes.toBytes("SampleCenterLocationY")) != null)
				    			wdp.dieSampleOriginY = Double.parseDouble(Bytes.toString(lastScan.getValue(Bytes.toBytes("y_info"), Bytes.toBytes("SampleCenterLocationY"))));
				    		get.addFamily(Bytes.toBytes("defect_info"));
				    		Result defect = devicetable.get(get);
//				    		Result defects1 = devicetable.get(get);
//							Cell[] cells = defects1.rawCells();
//							for(Cell cell: cells)
//							{
//								System.out.println("test1");
//								System.out.println(Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength())+"::"+
//										Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
//							}


//				    		Scan scan = new Scan();
//				    		ResultScanner defects2 = devicetable.getScanner(scan);
//							for(Result defect: defects2)
//							{
//								String[] s = Bytes.toString(defect.getValue()).split(",");
//							}

				    		for(Cell keyValue : defect.listCells()){
				    			String[] s = Bytes.toString(keyValue.getValueArray()).split(",");
				    			DefectProfile dp = new DefectProfile(wdp.defectList.size(), 1, 1);
				    			boolean good = false;
				    			for (int k = 0; k < s.length; k ++)
				    				if (s[k].split(":")[0].equals("XREL")) {
				    					dp.xRel = Double.parseDouble(s[k].split(":")[1]);
				    				}
				    				else if (s[k].split(":")[0].equals("YREL")) {
				    					dp.yRel = Double.parseDouble(s[k].split(":")[1]);
				    				}
				    				else if (s[k].split(":")[0].equals("XINDEX")) {
				    					dp.xIndex = Integer.parseInt(s[k].split(":")[1]);
				    				}
				    				else if (s[k].split(":")[0].equals("YINDEX")) {
				    					dp.yIndex = Integer.parseInt(s[k].split(":")[1]);
				    				}
				    				else if (s[k].split(":")[0].equals("TEST")) {
				    					if (!wdp.inspToolID.contains("RMAC")
				    							|| ((side.equals("") && Integer.parseInt(s[k].split(":")[1]) == 1) || (side.equals("B") && Integer.parseInt(s[k].split(":")[1]) == 9)))
				    						good = true;
				    				}
				    			if (good) {
				    				wdp.defectList.add(dp);
				    			}
				    		}
				    		wdp.transpose();
				    		wdp.freshPD(0);
				    		ImageIO.write(wdp.drawyMap(0, side), "jpg", new File(temp));
							  
				    		MimeBodyPart attachPart = new MimeBodyPart();
				    		attachPart.attachFile(temp);
				    		attachPart.setHeader("Content-ID", "rawimage");
				    		multipart.addBodyPart(attachPart);
				    		tempFiles.add(temp);
				    	}
				    }

				    for (int k = 0; k < xNo; k ++) {
				    	if (r.getValue(Bytes.toBytes("data"), Bytes.toBytes(side + "xImage" + (k + 1))) != null) {
				    		imgfilename = L.get(i).replace(' ', '_').replace(':', '_').replace('.', '_') + new Text("_" + (k + 1)).toString() + ".jpg";
				    		temp = "/tmp/" + imgfilename;
				    		OutputStream out = new BufferedOutputStream(new FileOutputStream(temp));
				    		out.write(r.getValue(Bytes.toBytes("data"), Bytes.toBytes(side + "xImage" + (k + 1))));
				    		out.close();
				    		MimeBodyPart attachPart = new MimeBodyPart();
				    		attachPart.attachFile(temp);
				    		attachPart.setHeader("Content-ID", cID[k]);
				    		multipart.addBodyPart(attachPart);
				    		tempFiles.add(temp);
				    	}
				    	if (r.getValue(Bytes.toBytes("data"), Bytes.toBytes(side + "AnalysisReport" + (k + 1))) != null) {
				    		rptfilename = L.get(i).replace(' ', '_').replace(':', '_').replace('.', '_') + new Text("_" + (k + 1)).toString() + ".csv";
				    		String temp1 = "/tmp/" + rptfilename; 
				    		OutputStream out1 = new BufferedOutputStream(new FileOutputStream(temp1));
				    		out1.write(r.getValue(Bytes.toBytes("data"), Bytes.toBytes(side + "AnalysisReport" + (k + 1))));
				    		out1.close();
				    		MimeBodyPart attachPart1 = new MimeBodyPart();
				    		attachPart1.attachFile(temp1);
				    		multipart.addBodyPart(attachPart1);
				    		tempFiles.add(temp1);
				    	}
				    }
				    
				    Date date = new Date();
		    		String s=buildMessage(L.get(i),Current_Date, element, DID, namespace, args[10], side, offset);
		    		System.out.println(date);
		    		char c = element[1].charAt(element[1].length() - 1);
		    		String daynight = "Day";
		    		if (Integer.parseInt(args[9]) < 12) {
		    			if (date.getHours() < Integer.parseInt(args[9]) || date.getHours() >= Integer.parseInt(args[9]) + 12)
		    				daynight = "Night";
		    		}
		    		else {
		    			if (date.getHours() >= Integer.parseInt(args[9]) || date.getHours() < Integer.parseInt(args[9]) - 12)
		    				daynight = "Night";
		    		}
		      message.setSubject(args[8] + (c == '1' || c == '7' ? (c=='1' ?  "N" : "W") : "")  + "_" + daynight + (side.equals("") ? " Front side" : " Back side") + " Mechanical x detected for y: "+element[2]+" (Device: " + DID + ", z ID: "+Current_zid+", Step ID: "+Current_Traveler+")" + (goodCatch ? "ALERT!" : ""));
		      //String s=printList(L);
		      // Now set the actual message
		      //String s = "<i>Greetings!</i><br>";
		      //s += "<b>Wish you a nice day!</b><br>";
		      //s += "<font color=red>Duke</font>";
		      
		      // creates multi-part
		    	/*
		    	MimeBodyPart imagePart = new MimeBodyPart();
				imagePart.setHeader("Content-ID", "AbcXyz123");
				imagePart.setDisposition(MimeBodyPart.INLINE);
				imagePart.attachFile(imageFilePath);
		    	 */
		
		     //message.addRecipient(Message.RecipientType.TO,new InternetAddress(to));
             System.out.println("arr1: " + Arrays.toString(arr1));
             System.out.println("arr2: " + Arrays.toString(arr2));
             System.out.println("arr3: " + Arrays.toString(arr3));
             if (tool.contains("KT28") || tool.contains("KT29") ||
                     tool.contains("KTPU") || tool.contains("KTBF") ||
                     tool.contains("KMAC") || tool.contains("RMAC")) {
                 if (goodCatch && arr1 != null){
                     for (j=0;j<arr1.length;j++)
                     {
                         message.addRecipient(Message.RecipientType.TO,
                                 new InternetAddress(arr1[j]));
                         System.out.println("come into primary_recipient");
                     }}
                 else if(!goodCatch && arr2 != null){
                     for (j=0;j<arr2.length;j++)
                     {
                         message.addRecipient(Message.RecipientType.TO, new InternetAddress(arr2[j]));
                         System.out.println("come into secondary_recipient");}}
             }
             else if (tool.contains("KSP"))
                {
                    for (j=0;j<arr3.length;j++)
                    {
                        message.addRecipient(Message.RecipientType.TO,
                                new InternetAddress(arr3[j]));
                        System.out.println("come into qual_recipient");
                    }
                }

//           if (goodCatch && arr1 != null)
//             {
//			     for (j=0;j<arr1.length;j++)
//			     {
//			    	   message.addRecipient(Message.RecipientType.TO,
//			    			   new InternetAddress(arr1[j]));
//			    	   System.out.println("come into primary_recipient");
//			     }
//             }
//		     else if (!goodCatch && arr2 != null)
//		     {
//		         for (j=0;j<arr2.length;j++)
//		         {
//		        	   message.addRecipient(Message.RecipientType.TO,
//		        			   new InternetAddress(arr2[j]));
//		        	   System.out.println("come into secondary_recipient");
//		         }
//		     }
		     System.out.println("goodCatch:"+ Boolean.toString(goodCatch));
		     MimeBodyPart messageBodyPart = new MimeBodyPart();
		     messageBodyPart.setContent(s, "text/html");
		     multipart.addBodyPart(messageBodyPart);
		     message.setContent(multipart);
		     //message.setText("This is the list of RowKeys: "+s);
		     wT.close();
		     // Send message
		
		     if (message.getAllRecipients() != null)
		    	 Transport.send(message);
		     for (String s1 : tempFiles)
		    	 if (Files.exists(Paths.get(s1)))
		    		 Files.delete(Paths.get(s1));
		     System.out.println("Sent message successfully....");}
			 }
			catch (MessagingException mex) {
				mex.printStackTrace();
			}
		}
		catch (Exception e) {
			e.printStackTrace();
		}	
	}
	
	private static String buildMessage(String a, String b, String[] c, String DID, String namespace, String days, String side, double offset) throws ParseException, IOException {
		Result r1=Row_Retrieve(a, namespace);
		String [] element = a.split("_");
		String LastScan="";
		String output = ""; //="<b>The Scan was Completed at the Current Time Stamp:&nbsp;</b>"+b+"<br>";
		output+="<b>The Row Key for the Current Scan:\t&nbsp;</b>"+a+"<br>";
		if(column_exists(r1,"LastScan_RowKey")) {
			LastScan= Bytes.toString(r1.getValue(Bytes.toBytes("data"),Bytes.toBytes("LastScan_RowKey")));
		}
		output += "<b>The Row Key for the Last Scan:\t&nbsp;</b>" + (LastScan == "" ? "No last scan" : LastScan) + "<br>";
		output += "<b>Result Timestamp:\t</b>" + c[0] + "<br>";
		output += "<b>Device:\t</b>" + DID + "<br>";
		output += "<b>z ID:\t</b>" + c[1] + "<br>";
		output += "<b>y ID:\t</b>" + c[2] + "<br>";
		output += "<b>Step ID:\t</b>" + c[3] + "<br>";
		 
	    int currentx_no=Integer.parseInt(Bytes.toString(r1.getValue(Bytes.toBytes("data"),Bytes.toBytes(side + "NO_xES"))));
	    for (int i = 1; i <= currentx_no; i ++) {
			byte [] current_value1 = r1.getValue(Bytes.toBytes("data"),Bytes.toBytes(side + "x"+i));	
			String currentx_info = Bytes.toString(current_value1);
			String[] cur_array2 = currentx_info.split(",R:");
			double current_dvalue=Double.parseDouble(cur_array2[1]) / 1000.0;
			current_dvalue=Math.round(current_dvalue * 100.0) / 100.0;
			String[] cur_array3 = cur_array2[0].split("THETA:");
			double current_thetavalue = (Double.parseDouble(cur_array3[1]) / Math.PI) * 180.0;
			current_thetavalue = Math.round(current_thetavalue * 100.0) / 100.0;
	        output+="<br>"; 
			output += "<b>-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------<br>";
			output+="<b><span style='background-color:yellow;'>Information for x Number:&nbsp;</b>" + i + "<br>";         
	        output+= "<span style='background-color:white;'>The D-Value for Current x:&nbsp;<span style='background-color:yellow;'>" + current_dvalue + "&nbsp;mm<br>";
	        output+= "<span style='background-color:white;'>The D-Value Range for Current x:&nbsp;<span style='background-color:yellow;'>" + (int)current_dvalue / 10 * 10 +"-" + ((int)current_dvalue / 10 * 10 + 10) + "&nbsp;mm<br>";
	        output+= "<span style='background-color:white;'>The Theta-Value for Current x:&nbsp;"+current_thetavalue+"&nbsp;deg<br><b><br>";
	        output += "<b>y Image with notch DOWN";
	        if (!LastScan.equals(""))
	        	output += "_______________________________________________y Image of Last Scan";
	        output += "<br>";
	        output += "<img src=\"cid:" + cID[i - 1] + "\" alt=\"Image not found\" height=\"600\" width=\"600\">";
	        if (!LastScan.equals(""))
	        	output += "<img src=\"cid:rawimage\" alt=\"Image not found\" height=\"600\" width=\"600\">";
	        output += "<br>";
	        output+="<br>";
	        output+="<b><span style='background-color:yellow;'>The Suspected Toolist from last scan with similar D-value (+-" + offset + "mm) in the last "+ days +" days</span></b> <br/>";
	        output+="<br>";
	        output+= printList(column_retrieve(a, i, side + "Final_ToolList", namespace), column_retrieve(a, i, side + "AREA_LIST", namespace));
	        output+="<br>";
	        output+="<b><span style='background-color:yellow;'>Refer to attached report for complete report</span></b> <br/>";
	        output+="<br>";
	
	    }
		return output;
	}

	private static ArrayList<String> column_retrieve(String CurrentRowKey,int xno,String column, String namespace) throws ParseException {//used to retrieve tool list given row key and x number
		try  {
			//retrieve all tools  
			Result r1=Row_Retrieve(CurrentRowKey, namespace);
			byte [] value = r1.getValue(Bytes.toBytes("data"),Bytes.toBytes(column+xno));
			String toolstring =Bytes.toString(value);
			String[] splitArray1 = toolstring.split(",");
			if (splitArray1[0].equals("")||toolstring.isEmpty()) {
				splitArray1[0]="NA";
			}
			ArrayList<String> ToolList = new ArrayList<String>(Arrays.asList(splitArray1));
			return ToolList;
		} catch( NullPointerException e) {
			ArrayList<String> error =new ArrayList<String>(Arrays.asList("Null pointer"));
			return error;
		}
	}

	private static ArrayList<String> RetrieveRowKeys(String starttime, String endtime,String flag, String namespace, String side) {
		Configuration conf = HBaseConfiguration.create();
		try {
            Connection conn = ConnectionFactory.createConnection(conf);
            String xHTName = namespace + ":x";
            Table table = conn.getTable(TableName.valueOf(xHTName.getBytes()));
			ArrayList <String> rowkeylist=new ArrayList<String>();
			new ArrayList<String>();
			Scan scan = new Scan();
			scan.setStartRow(starttime.getBytes());
            scan.setStopRow(endtime.getBytes());
			ResultScanner r = table.getScanner(scan);
			//System.out.println("LastScan date before check " + LastScan_Date);
			// Scanning through 4 weeks of data
			for(Result r2 : r) {
				String rowkey=Bytes.toString(r2.rawCells()[0].getRowArray(),r2.rawCells()[0].getRowOffset(),r2.rawCells()[0].getRowLength());
				//System.out.println(rowkey);
				if (flag.equals("1")) {
					if(column_exists(r2, side + "NO_xES") && !Bytes.toString(r2.getValue(Bytes.toBytes("data"), Bytes.toBytes(side + "NO_xES"))).contains("0")
						&& !(column_exists(r2, side + "Email_Status") && Bytes.toString(r2.getValue(Bytes.toBytes("data"), Bytes.toBytes(side + "Email_Status"))).contains("1"))) {
						rowkeylist.add(rowkey);
					}
				}
				else if (flag.equals("0")) {
					if(column_exists(r2, side + "NO_xES") && !Bytes.toString(r2.getValue(Bytes.toBytes("data"), Bytes.toBytes(side + "NO_xES"))).contains("0")) {
							rowkeylist.add(rowkey);
						}
					}
			}
			for (String key : rowkeylist) {
				Put p = new Put(Bytes.toBytes(key));
				p.addColumn(Bytes.toBytes("data"), Bytes.toBytes(side + "Email_Status"),Bytes.toBytes("1"));
				table.put(p);
			}
			table.close();
			return rowkeylist;
		} catch (Exception e) {
			e.printStackTrace();
			ArrayList <String> rowkeylist=new ArrayList<String>();
			rowkeylist.add("");
			return rowkeylist;
		}
	}
	
	private static Result Row_Retrieve (String key, String namespace) {//returns result object given a row key
		Configuration conf = HBaseConfiguration.create();
		try{
		    Connection conn = ConnectionFactory.createConnection(conf);
		    String xHTName = namespace + ":x";
		    Table table = conn.getTable(TableName.valueOf(xHTName.getBytes()));
            Filter filter = new PrefixFilter(Bytes.toBytes(key));
            Get g = new Get(Bytes.toBytes(key));
            Result r1 = table.get(g);
			/*Scan scan1 = new Scan();
			scan1.setFilter(filter);
			ResultScanner scanner = table.getScanner(scan1);
			Result r1=scanner.next();
			*/
            return r1;
		}catch (IOException e){
            e.printStackTrace();
            Result r1=new Result();
            return r1;
        }
	}
	
	private static boolean column_exists(Result r1, String s) {// checks if column exists in HBASE record
		Cell[] rawCells = r1.rawCells();
		for(Cell c:rawCells) {
			if (Bytes.toString(c.getQualifierArray(),c.getQualifierOffset(),c.getQualifierLength()).equals(s)) {
				return true;
			}
		}
		return false;
	}
	
	private static String printList(ArrayList<String> s,ArrayList<String> r) {// method to print the contents of a list
		String prefix = "<html><body><table border=\"1\">\n";
		final StringBuilder sb = new StringBuilder(prefix);
		sb.append("<tr>");
		sb.append("<th>");
		sb.append("Area");
		sb.append("</th>");
		sb.append("<th>");
		sb.append("Tool ID");
		sb.append("</th>");
		sb.append("<th>");
		sb.append("y Hit Count");
		sb.append("</th>");
		sb.append("<th>");
		sb.append("z Hit Count");
		sb.append("</th>");
		sb.append("</tr>\n");
		
		if(s.get(0).equals("NA") || s.get(0).equals("") || s.get(0).equals("Null pointer")) {
			sb.append("<tr>");
			sb.append("<td align=\"left\">");
		    sb.append("No tools matched");
		    sb.append("</td>");
		    sb.append("</tr>\n");
		}
		else if (s.get(0).contains("Repeated")) {
			sb.append("<tr>");
			sb.append("<td align=\"left\">");
		    sb.append("Same x as last scan");
		    sb.append("</td>");
		    sb.append("</tr>\n");
		    }
		else {
			if(s.size()<=r.size()) {
				for ( int i=0;i<s.size();i++) {
					String[] splitArray1 = s.get(i).replace('(', ' ').replace(')', ' ').split(" ");
					sb.append("<tr>");
					sb.append("<td align=\"left\">");
					sb.append("" + (i < r.size() ? r.get(i) : "no data"));
					sb.append("</td>");
					sb.append("<td width = 150>");
					sb.append(" " + splitArray1[0]);
					sb.append("</td>");
					sb.append("<td width = 150>");
					sb.append("" + splitArray1[1]);
					sb.append("</td>");
					sb.append("<td width = 150>");
					sb.append("" + (2 < splitArray1.length ? splitArray1[2] : "no data"));
					sb.append("</td>");
					sb.append("</tr>\n");
				}
			}
		}
		sb.append("</table>");
		sb.append("</body>");
		sb.append("</html>");
		return sb.toString();
	}
}