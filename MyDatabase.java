/*
 * This file parses the PHARMA_TRIALS_1000B.csv file and stores 
 * binary database into 'data.db' file
 */
package mydatabase;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import com.csvreader.CsvReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.TreeMap;

/**
 *
 * @author binalkamani
 */
public class MyDatabase {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws FileNotFoundException, IOException {

        String basePath = new File("").getAbsolutePath();
        String op_path = basePath;
        String ndx_path = basePath;
        
        System.out.println(basePath);
        
        System.out.println("Please select one of the options:");
        System.out.println("Enter 1 to parse the csv file");
        System.out.println("Enter 2 to run the query:");
        
        Scanner one = new Scanner(System.in);
        Scanner four = new Scanner(System.in);
                     
       RandomAccessFile opfile = new RandomAccessFile(op_path + "\\data.db", "rw");
        
        int no = one.nextInt();
        
        if (no == 1){
        
            String csv_path=basePath.concat("\\PHARMA_TRIALS_1000B.csv");
            System.out.println("csv_path: " + csv_path);
           //CSV Reader Object to read PHARMA_TRIALS_1000B.csv file
            CsvReader csvfile = new CsvReader(csv_path);
            
            // create 11 map for 11 index files
            Map<Integer, List<Long>> map_id = new HashMap<Integer, List<Long>>();
            Map<String, List<Long>> map_com = new HashMap<String, List<Long>>();
            Map<String, List<Long>> map_drug = new HashMap<String, List<Long>>();
            Map<Integer, List<Long>> map_trials = new HashMap<Integer, List<Long>>();
            Map<Integer, List<Long>> map_patients = new HashMap<Integer, List<Long>>();
            Map<Integer, List<Long>> map_dosage = new HashMap<Integer, List<Long>>();
            Map<Float, List<Long>> map_reading = new HashMap<Float, List<Long>>();
            Map<String, List<Long>> map_db = new HashMap<String, List<Long>>();
            Map<String, List<Long>> map_cs = new HashMap<String, List<Long>>();
            Map<String, List<Long>> map_gf = new HashMap<String, List<Long>>();
            Map<String, List<Long>> map_fa = new HashMap<String, List<Long>>();        

            opfile.seek(0);
            csvfile.readHeaders();
            
                //Reading file PHARMA_TRIALS_1000B.csv  
           while (csvfile.readRecord())
            {
                int four_field = 0;
                int ID = Integer.parseInt(csvfile.get("id"));
                long fp = opfile.getFilePointer();
                opfile.writeInt(ID);
                if (map_id.containsKey(ID) == false){
                    List<Long> List_id = new ArrayList<Long>();
                    map_id.put(ID, List_id);
                }
                map_id.get(ID).add(fp);
                String company = csvfile.get("company");
                int length = company.length();
                if (map_com.containsKey(company) == false){
                    List<Long> List_com = new ArrayList<Long>();
                    map_com.put(company, List_com);
                }
                map_com.get(company).add(fp);
                opfile.writeByte(length);
                opfile.writeBytes(company);

                String drugID = csvfile.get("drug_id");
                if (map_drug.containsKey(drugID) == false){
                    List<Long> List_drug = new ArrayList<>();
                    map_drug.put(drugID, List_drug);
                }
                map_drug.get(drugID).add(fp);
                opfile.writeBytes(drugID);

                short trial_s = Short.parseShort(csvfile.get("trials"));
                int trial = Integer.parseInt(csvfile.get("trials"));
                if (map_trials.containsKey(trial) == false){
                    List<Long> List_trials = new ArrayList<Long>();
                    map_trials.put(trial, List_trials);
                }
                map_trials.get(trial).add(fp);
                opfile.writeShort(trial_s);

                short patients_s = Short.parseShort(csvfile.get("patients"));
                int patients = Integer.parseInt(csvfile.get("patients"));
                if (map_patients.containsKey(patients) == false){
                    List<Long> List_patients = new ArrayList<Long>();
                    map_patients.put(patients, List_patients);
                }
                map_patients.get(patients).add(fp);
                opfile.writeShort(patients_s);

                short dosage_mgs = Short.parseShort(csvfile.get("dosage_mg"));
                int dosage_mg = Integer.parseInt(csvfile.get("dosage_mg"));
                if (map_dosage.containsKey(dosage_mg) == false){
                    List<Long> List_dosage = new ArrayList<Long>();
                    map_dosage.put(dosage_mg, List_dosage);
                }
                map_dosage.get(dosage_mg).add(fp); 
                opfile.writeShort(dosage_mgs);

                float reading = Float.parseFloat(csvfile.get("reading"));
                if (map_reading.containsKey(reading) == false){
                    List<Long> List_reading = new ArrayList<Long>();
                    map_reading.put(reading, List_reading);
                }
                map_reading.get(reading).add(fp);
                opfile.writeFloat(reading);

                String double_blind = csvfile.get("double_blind");
                if (double_blind.equals("TRUE")){
                    four_field = four_field + 8;
                }
                if (map_db.containsKey(double_blind) == false){
                    List<Long> List_db = new ArrayList<Long>();
                    map_db.put(double_blind, List_db);
                }
                map_db.get(double_blind).add(fp);

                String controlled_study = csvfile.get("controlled_study");
                if (controlled_study.equals("TRUE")){
                    four_field = four_field + 4;
                }
                if (map_cs.containsKey(controlled_study) == false){
                    List<Long> List_cs = new ArrayList<Long>();
                    map_cs.put(controlled_study, List_cs);
                }
                map_cs.get(controlled_study).add(fp);

                String govt_funded = csvfile.get("govt_funded");
                if (govt_funded.equals("TRUE")){
                    four_field = four_field + 2;
                }
                if (map_gf.containsKey(govt_funded) == false){
                    List<Long> List_gf = new ArrayList<Long>();
                    map_gf.put(govt_funded, List_gf);
                }
                map_gf.get(govt_funded).add(fp);

                String fda_approved = csvfile.get("fda_approved");
                if (fda_approved.equals("TRUE")){
                    four_field = four_field + 1;
                }          
                 if (map_fa.containsKey(fda_approved) == false){
                    List<Long> List_fa = new ArrayList<Long>();
                    map_fa.put(fda_approved, List_fa);
                }
                map_fa.get(fda_approved).add(fp);

                opfile.writeByte(four_field);

                //Sorting all hashmap
                TreeMap<Integer, List<Long>> sortedmap_id = new TreeMap<Integer, List<Long>>(map_id); 
                TreeMap<String, List<Long>> sortedmap_com = new TreeMap<String, List<Long>>(map_com);

                TreeMap<String, List<Long>> sortedmap_drug = new TreeMap<String, List<Long>>(map_drug);
                TreeMap<Integer, List<Long>> sortedmap_trials = new TreeMap<Integer, List<Long>>(map_trials);
                TreeMap<Integer, List<Long>> sortedmap_patients = new TreeMap<Integer, List<Long>>(map_patients);
                TreeMap<Integer, List<Long>> sortedmap_dosage = new TreeMap<Integer, List<Long>>(map_dosage);

                TreeMap<Float, List<Long>> sortedmap_reading = new TreeMap<Float, List<Long>>(map_reading);
                TreeMap<String, List<Long>> sortedmap_db = new TreeMap<String, List<Long>>(map_db);
                TreeMap<String, List<Long>> sortedmap_cs = new TreeMap<String, List<Long>>(map_cs);
                TreeMap<String, List<Long>> sortedmap_gf = new TreeMap<String, List<Long>>(map_gf);
                TreeMap<String, List<Long>> sortedmap_fa = new TreeMap<String, List<Long>>(map_fa);

             //Serializing id map
              try{
                File file_id=new File(ndx_path + "\\id.ndx");
                FileOutputStream fos_id=new FileOutputStream(file_id);
                ObjectOutputStream oos_id=new ObjectOutputStream(fos_id);

                oos_id.writeObject(sortedmap_id);
                oos_id.flush();
                oos_id.close();
                fos_id.close();
            }catch(Exception e){
                System.out.println("Exception writing an index file for id");
                }

             //Serializing company map
              try{
                File file_com=new File(ndx_path + "\\company.ndx");
                FileOutputStream fos_com=new FileOutputStream(file_com);
                ObjectOutputStream oos_com=new ObjectOutputStream(fos_com);

                oos_com.writeObject(sortedmap_com);
                oos_com.flush();
                oos_com.close();
                fos_com.close();
            }catch(Exception e){
                System.out.println("Exception writing an index file for company");
                }

             //Serializing drug_id map
              try{
                File file_drug=new File(ndx_path + "\\drug_id.ndx");
                FileOutputStream fos_drug=new FileOutputStream(file_drug);
                ObjectOutputStream oos_drug=new ObjectOutputStream(fos_drug);

                oos_drug.writeObject(sortedmap_drug);
                oos_drug.flush();
                oos_drug.close();
                fos_drug.close();
            }catch(Exception e){
                System.out.println("Exception writing an index file for drug_id");
                }

             //Serializing trials map
              try{
                File file_trials=new File(ndx_path + "\\trials.ndx");
                FileOutputStream fos_trials=new FileOutputStream(file_trials);
                ObjectOutputStream oos_trials=new ObjectOutputStream(fos_trials);

                oos_trials.writeObject(sortedmap_trials);
                oos_trials.flush();
                oos_trials.close();
                fos_trials.close();
            }catch(Exception e){
                System.out.println("Exception writing an index file for trials");
                }   

              //Serializing patients map
              try{
                File file_patients=new File(ndx_path + "\\patients.ndx");
                FileOutputStream fos_patients=new FileOutputStream(file_patients);
                ObjectOutputStream oos_patients=new ObjectOutputStream(fos_patients);

                oos_patients.writeObject(sortedmap_patients);
                oos_patients.flush();
                oos_patients.close();
                fos_patients.close();
            }catch(Exception e){
                System.out.println("Exception writing an index file for patients");
                } 

              //Serializing dosage map
              try{
                File file_dosage=new File(ndx_path + "\\dosage.ndx");
                FileOutputStream fos_dosage=new FileOutputStream(file_dosage);
                ObjectOutputStream oos_dosage=new ObjectOutputStream(fos_dosage);

                oos_dosage.writeObject(sortedmap_dosage);
                oos_dosage.flush();
                oos_dosage.close();
                fos_dosage.close();
            }catch(Exception e){
                System.out.println("Exception writing an index file for dosage");
                } 

              //Serializing reading map
              try{
                File file_reading=new File(ndx_path + "\\reading.ndx");
                FileOutputStream fos_reading=new FileOutputStream(file_reading);
                ObjectOutputStream oos_reading=new ObjectOutputStream(fos_reading);

                oos_reading.writeObject(sortedmap_reading);
                oos_reading.flush();
                oos_reading.close();
                fos_reading.close();
            }catch(Exception e){
                System.out.println("Exception writing an index file for reading");
                }

              //Serializing double_blind map
              try{
                File file_db=new File(ndx_path + "\\double_blind.ndx");
                FileOutputStream fos_db=new FileOutputStream(file_db);
                ObjectOutputStream oos_db=new ObjectOutputStream(fos_db);

                oos_db.writeObject(sortedmap_db);
                oos_db.flush();
                oos_db.close();
                fos_db.close();
            }catch(Exception e){
                System.out.println("Exception writing an index file for double_blind");
                }

              //Serializing controlled_Study map
              try{
                File file_cs=new File(ndx_path + "\\controlled_study.ndx");
                FileOutputStream fos_cs=new FileOutputStream(file_cs);
                ObjectOutputStream oos_cs=new ObjectOutputStream(fos_cs);

                oos_cs.writeObject(sortedmap_cs);
                oos_cs.flush();
                oos_cs.close();
                fos_cs.close();
            }catch(Exception e){
                System.out.println("Exception writing an index file for controlled_study");
                }          

              //Serializing govt_funded map
              try{
                File file_gf=new File(ndx_path + "\\govt_funded.ndx");
                FileOutputStream fos_gf=new FileOutputStream(file_gf);
                ObjectOutputStream oos_gf=new ObjectOutputStream(fos_gf);

                oos_gf.writeObject(sortedmap_gf);
                oos_gf.flush();
                oos_gf.close();
                fos_gf.close();
            }catch(Exception e){
                System.out.println("Exception writing an index file for govt_funded");
                }

              //Serializing fda_approved map
              try{
                File file_fa=new File(ndx_path + "\\fda_approved.ndx");
                FileOutputStream fos_fa=new FileOutputStream(file_fa);
                ObjectOutputStream oos_fa=new ObjectOutputStream(fos_fa);

                oos_fa.writeObject(sortedmap_fa);
                oos_fa.flush();
                oos_fa.close();
                fos_fa.close();
            }catch(Exception e){
                System.out.println("Exception writing an index file for fda_approved");
                    }
    
            }
          csvfile.close(); 
        }
        
      
    else if (no == 2){
            Scanner five = new Scanner(System.in);
            String user_ans;
            
            if (opfile.length() != 0){
                do  {
                        System.out.println("Enter one of the number for a field to run query on from following options:");
                        System.out.println("1:id, 2:company, 3:drug_id, 4:trials, 5:patients, ");
                        System.out.println("6:dosage_mg, 7:reading, 8:double_blind, 9:controlled study, 10: govt_funded, 11:fda_approved");
                        if (four.hasNextInt() == true){
                            int qr_option = four.nextInt();
                            get_query(qr_option, opfile, ndx_path);
                        }
                        else{
                            System.out.println("Enter valid integer entry");
                        }
                        System.out.println("Want to run a new query?? Reply with 'y' to continue and any other key to exit");
      
                        if (five.hasNextLine() == true){
                            user_ans = five.nextLine();
                        }
                        else
                        {
                            user_ans = "n";
                        }                           
                    }while("y".equalsIgnoreCase(user_ans));       
                }
                else{
                    System.out.println("Make sure the csv file is parsed and path entered is correct!");
                }           
      } 
        else{
            System.out.println("Please enter valid entry");
            }
        
        System.out.println();                       
    }

    private static void id_query(RandomAccessFile opfile, String ndx_path) {
        try{
            File toRead=new File(ndx_path + "\\id.ndx");
            int_query_call(toRead, opfile);
        }catch(Exception e){
            System.out.println("Please enter valid entry");
            }
    }
  
    private static void drug_query(RandomAccessFile opfile, String ndx_path) {
    try{
        File toRead=new File(ndx_path + "\\drug_id.ndx");
        string_query_call(toRead, opfile);
        }catch(Exception e){
            System.out.println("Please enter valid entry");
        }
    }

    private static void trials_query(RandomAccessFile opfile, String ndx_path) {
        try{
            File toRead=new File(ndx_path + "\\trials.ndx");
            int_query_call(toRead, opfile);
            }catch(Exception e){
                System.out.println("Please enter valid entry");
            }
    }

    private static void company_query(RandomAccessFile opfile, String ndx_path) {

        try{
            File toRead=new File(ndx_path + "\\company.ndx");
            string_query_call(toRead, opfile);
        }catch(Exception e){
            System.out.println("Please enter valid entry");
            }
    }

    private static void patients_query(RandomAccessFile opfile, String ndx_path) {
        try{
            File toRead=new File(ndx_path + "\\patients.ndx");
            int_query_call(toRead, opfile);
            }catch(Exception e){
                System.out.println("Please enter valid entry");
            }
    }

    private static void dosage_query(RandomAccessFile opfile, String ndx_path) {
        //Reading file reading.ndx
        try{
            File toRead=new File(ndx_path + "\\dosage.ndx");
            int_query_call(toRead, opfile);
        }catch(Exception e){
            System.out.println("Please enter valid entry");
            }
    }
    private static void db_query(RandomAccessFile opfile, String ndx_path) {
        try{
            File toRead=new File(ndx_path + "\\double_blind.ndx");
            string_query_call(toRead, opfile);
            }catch(Exception e){
              System.out.println("Please enter valid entry");
            }
    }

    private static void cs_query(RandomAccessFile opfile, String ndx_path) {
            try{
                File toRead=new File(ndx_path + "\\controlled_study.ndx");
                string_query_call(toRead, opfile);
            }catch(Exception e){
                System.out.println("Please enter valid entry");
                }
    }

    private static void gf_query(RandomAccessFile opfile, String ndx_path) {
            try{
                File toRead=new File(ndx_path + "\\govt_funded.ndx");
                string_query_call(toRead, opfile);
            }catch(Exception e){
                System.out.println("Please enter valid entry");
                }
    }

    private static void fa_query(RandomAccessFile opfile, String ndx_path) {
        try{
            File toRead=new File(ndx_path + "\\fda_approved.ndx");
            string_query_call(toRead, opfile);
            }catch(Exception e){
            System.out.println("Please enter valid entry");
            }
    }
  
    
    private static void reading_query(RandomAccessFile opfile, String ndx_path) throws FileNotFoundException {
            //Reading file reading.ndx
            try{
                int count = 0;
                File toRead=new File(ndx_path + "\\reading.ndx");
                FileInputStream fis=new FileInputStream(toRead);
                ObjectInputStream ois=new ObjectInputStream(fis);
                Scanner reading_or = new Scanner(System.in);
                System.out.println("Enter operator: ");
                String or = reading_or.nextLine();
                System.out.println("Enter value: ");
                Float f = reading_or.nextFloat();

                TreeMap<Float,List<Long>> mapInFile=(TreeMap<Float,List<Long>>)ois.readObject();
                ois.close();
                fis.close();
                boolean flag = false;
                //print All data in MAP
                for(Map.Entry<Float,List<Long>> m :mapInFile.entrySet()){
                    switch(or){
                        case "=":{
                            if (f.equals(m.getKey())){
                               // System.out.println("equal case");  
                                flag=true;}
                            break;}
                        case ">":{
                            if (m.getKey()>f){
                                //System.out.println("equal case");  
                                flag=true;}
                            break;}
                        case "<":{
                            if (m.getKey()<f){
                                //System.out.println("equal case");  
                                flag=true;}
                            break;}    
                        case ">=":{
                            if (m.getKey()>=f){
                                //System.out.println("equal case");  
                                flag=true;}
                            break;}
                        case "<=":{
                            if (m.getKey()<=f){
                                //System.out.println("equal case");  
                                flag=true;}
                            break;}
                        case "!=":{
                            flag=true;
                            if (f.equals(m.getKey())){
                                //System.out.println("equal case");  
                                flag=false;}
                            break;}   
                        default:{
                            if (f.equals(m.getKey()))
                                //System.out.println("default case");
                                flag=false;
                            break;
                             }
                        }
                    if (flag == true)
                    {
                        flag=false;
                       // System.out.println(m.getKey()+" : "+m.getValue());
                        for (int i = 0; i < m.getValue().size(); i++) {
                            count++;
                            opfile.seek(m.getValue().get(i));
                            System.out.print("record #" + count + ":");
                            System.out.print("" + opfile.readInt());
                            System.out.print("|");
                            int com_len = opfile.readByte();
                            for (int l=0; l<com_len; l++){
                            System.out.print("" + (char)opfile.readByte());}
                            System.out.print("|");
                            for (int l=0; l<6; l++){
                            System.out.print("" + (char)opfile.readByte());}
                            System.out.print("|" + opfile.readShort());
                            System.out.print("|" + opfile.readShort());
                            System.out.print("|" + opfile.readShort());
                            System.out.print("|" + opfile.readFloat());
                            System.out.print("|");
                            final byte double_blind_mask      = 8;    // binary 0000 1000
                            final byte controlled_study_mask  = 4;    // binary 0000 0100
                            final byte govt_funded_mask       = 2;    // binary 0000 0010
                            final byte fda_approved_mask      = 1;    // binary 0000 0001
                            byte decode_byte = opfile.readByte();
                            /* Use bitwise AND to get 2^3 bit */
                            //System.out.print("double_blind bit is:     ");
                            System.out.print(double_blind_mask == (byte)(decode_byte & double_blind_mask));
                            System.out.print("|");
                            /* Use bitwise AND to get 2^2 bit */
                            //System.out.print("controlled_study bit is: ");
                            System.out.print(controlled_study_mask == (byte)(decode_byte & controlled_study_mask));
                            System.out.print("|");
                            /* Use bitwise AND to get 2^1 bit */
                            //System.out.print("govt_funded bit is:      ");
                            System.out.print(govt_funded_mask == (byte)(decode_byte & govt_funded_mask));
                            System.out.print("|");
                            /* Use bitwise AND to get 2^0 bit */
                            //System.out.print("fda_approved bit is:     ");
                            System.out.print(fda_approved_mask == (byte)(decode_byte & fda_approved_mask));
                            System.out.println("");
                            }
                        }
                    }
                }catch(Exception e){
                    System.out.println("Make sure file exists at " + ndx_path);
                    }
    }
    
    private static void int_query_call(File toRead, RandomAccessFile opfile) throws FileNotFoundException, IOException, ClassNotFoundException{
            
            int count = 0;
            FileInputStream fis=new FileInputStream(toRead);
            ObjectInputStream ois=new ObjectInputStream(fis);
            Scanner id_or = new Scanner(System.in);
            System.out.println("Enter operator: ");
            String or = id_or.nextLine();
            System.out.println("Enter value: ");
            int d = id_or.nextInt();

            TreeMap<Integer,List<Long>> mapInFile=(TreeMap<Integer,List<Long>>)ois.readObject();
            ois.close();
            fis.close();
            boolean flag = false;
            //print All data in MAP
            for(Map.Entry<Integer,List<Long>> m :mapInFile.entrySet()){
                switch(or){
                    case "=":{
                        if (m.getKey() == d){
                           // System.out.println("equal case");  
                            flag=true;}
                        break;}
                    case ">":{
                        if (m.getKey()>d){
                            //System.out.println("equal case");  
                            flag=true;}
                        break;}
                    case "<":{
                        if (m.getKey()<d){
                            //System.out.println("equal case");  
                            flag=true;}
                        break;}    
                    case ">=":{
                        if (m.getKey()>=d){
                            //System.out.println("equal case");  
                            flag=true;}
                        break;}
                    case "<=":{
                        if (m.getKey()<=d){
                            //System.out.println("equal case");  
                            flag=true;}
                        break;}
                    case "!=":{
                        flag=true;
                        if (m.getKey() == d){
                            //System.out.println("equal case");  
                            flag=false;}
                        break;}   
                    default:{
                            flag=false;
                        break;
                         }
                    }
                if (flag == true)
                {
                    flag=false;
                   // System.out.println(m.getKey()+" : "+m.getValue());
                    for (int i = 0; i < m.getValue().size(); i++) {
                        count++;
                        opfile.seek(m.getValue().get(i));
                        System.out.print("record #" + count + ":");
                        System.out.print("" + opfile.readInt());
                        System.out.print("|");
                        int com_len = opfile.readByte();
                        for (int l=0; l<com_len; l++){
                        System.out.print("" + (char)opfile.readByte());}
                        System.out.print("|");
                        for (int l=0; l<6; l++){
                        System.out.print("" + (char)opfile.readByte());}
                        System.out.print("|" + opfile.readShort());
                        System.out.print("|" + opfile.readShort());
                        System.out.print("|" + opfile.readShort());
                        System.out.print("|" + opfile.readFloat());
                        System.out.print("|");
                        final byte double_blind_mask      = 8;    // binary 0000 1000
                        final byte controlled_study_mask  = 4;    // binary 0000 0100
                        final byte govt_funded_mask       = 2;    // binary 0000 0010
                        final byte fda_approved_mask      = 1;    // binary 0000 0001
                        byte decode_byte = opfile.readByte();
                        /* Use bitwise AND to get 2^3 bit */
                        //System.out.print("double_blind bit is:     ");
                        System.out.print(double_blind_mask == (byte)(decode_byte & double_blind_mask));
                        System.out.print("|");
                        /* Use bitwise AND to get 2^2 bit */
                        //System.out.print("controlled_study bit is: ");
                        System.out.print(controlled_study_mask == (byte)(decode_byte & controlled_study_mask));
                        System.out.print("|");
                        /* Use bitwise AND to get 2^1 bit */
                        //System.out.print("govt_funded bit is:      ");
                        System.out.print(govt_funded_mask == (byte)(decode_byte & govt_funded_mask));
                        System.out.print("|");
                        /* Use bitwise AND to get 2^0 bit */
                        //System.out.print("fda_approved bit is:     ");
                        System.out.print(fda_approved_mask == (byte)(decode_byte & fda_approved_mask));
                        System.out.println("");
                        }
                    }
            }
    }
    
    private static void string_query_call(File toRead, RandomAccessFile opfile) throws FileNotFoundException, IOException, ClassNotFoundException{
            int count=0;
            FileInputStream fis=new FileInputStream(toRead);
            ObjectInputStream ois=new ObjectInputStream(fis);
            Scanner com_or = new Scanner(System.in);
            System.out.println("Enter operator: ");
            String or = com_or.nextLine();
            System.out.println("Enter value: ");
            String s = com_or.nextLine();

            TreeMap<String,List<Long>> mapInFile=(TreeMap<String,List<Long>>)ois.readObject();
            ois.close();
            fis.close();
            boolean flag = false;
            //print All data in MAP
            for(Map.Entry<String,List<Long>> m :mapInFile.entrySet()){
                switch(or){
                    case "=":{
                        if (s.equalsIgnoreCase(m.getKey())){
                           // System.out.println("equal case");  
                            flag=true;}
                        break;}

                    case "!=":{
                        flag=true;
                        if (s.equalsIgnoreCase(m.getKey())){
                            //System.out.println("equal case");  
                            flag=false;}
                        break;}   
                    default:{
                            flag=false;
                        break;
                         }
                    }
                if (flag == true)
                {
                    flag=false;
                   // System.out.println(m.getKey()+" : "+m.getValue());
                    for (int i = 0; i < m.getValue().size(); i++) {
                        count++;
                        opfile.seek(m.getValue().get(i));
                        System.out.print("record #" + count + ":");
                        System.out.print("" + opfile.readInt());
                        System.out.print("|");
                        int com_len = opfile.readByte();
                        for (int l=0; l<com_len; l++){
                        System.out.print("" + (char)opfile.readByte());}
                        System.out.print("|");
                        for (int l=0; l<6; l++){
                        System.out.print("" + (char)opfile.readByte());}
                        System.out.print("|" + opfile.readShort());
                        System.out.print("|" + opfile.readShort());
                        System.out.print("|" + opfile.readShort());
                        System.out.print("|" + opfile.readFloat());
                        System.out.print("|");
                        final byte double_blind_mask      = 8;    // binary 0000 1000
                        final byte controlled_study_mask  = 4;    // binary 0000 0100
                        final byte govt_funded_mask       = 2;    // binary 0000 0010
                        final byte fda_approved_mask      = 1;    // binary 0000 0001
                        byte decode_byte = opfile.readByte();
                        /* Use bitwise AND to get 2^3 bit */
                        //System.out.print("double_blind bit is:     ");
                        System.out.print(double_blind_mask == (byte)(decode_byte & double_blind_mask));
                        System.out.print("|");
                        /* Use bitwise AND to get 2^2 bit */
                        //System.out.print("controlled_study bit is: ");
                        System.out.print(controlled_study_mask == (byte)(decode_byte & controlled_study_mask));
                        System.out.print("|");
                        /* Use bitwise AND to get 2^1 bit */
                        //System.out.print("govt_funded bit is:      ");
                        System.out.print(govt_funded_mask == (byte)(decode_byte & govt_funded_mask));
                        System.out.print("|");
                        /* Use bitwise AND to get 2^0 bit */
                        //System.out.print("fda_approved bit is:     ");
                        System.out.print(fda_approved_mask == (byte)(decode_byte & fda_approved_mask));
                        System.out.println("");
                        }
                    }

            }
    }
    private static void get_query(int qr_option, RandomAccessFile opfile, String ndx_path) throws FileNotFoundException {
        
        switch(qr_option){
                case 1:
                    id_query(opfile, ndx_path);
                    break;
                case 2:
                    company_query(opfile, ndx_path);
                    break;
                case 3:
                    drug_query(opfile, ndx_path);
                    break;
                case 4:
                    trials_query(opfile, ndx_path);
                    break;
                case 5:
                    patients_query(opfile, ndx_path);
                    break;
                case 6:
                    dosage_query(opfile, ndx_path);
                    break;
                case 7:
                    reading_query(opfile, ndx_path);    
                    break;
                case 8:
                    db_query(opfile, ndx_path);
                    break;
                case 9:
                    cs_query(opfile, ndx_path);
                    break;
                case 10:
                    gf_query(opfile, ndx_path);
                    break;
                case 11:
                    fa_query(opfile, ndx_path);
                    break;
                default:
                    System.out.println("Please enter valid option");
                    break;
        }
    }    
}
