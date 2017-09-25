using System;

namespace csv_test
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Hello World!");
        }

        static public void BulkCopyToSQL_OLEDB(string fileName, string tableName, string tireOrWheel)
        {
            /* Use ONLY file path. Two other important points:
                1. HDR=YES means, you have column names in the CSV file. If your CSV file does not contain column names, set it to HDR=NO
                2. I also placed a text file named schema.ini in the same folder as I upload my CSV files. Down below, I'll show you what's in that file.
                    schema.ini file simply describes the data structure in the CSV file.
            */
            //string strCSVConnection = "Provider=Microsoft.Jet.OLEDB.4.0;Data Source=" + FitmentsFolderPath + ";" + "Extended Properties='text;HDR=YES;'";

            try
            {
                // Here we're creating the bulkCopy and opening a connection to the database
                using (var reader = File.OpenText(fileName))
                using (var csv = new CsvReader(reader))
                using (SqlBulkCopy bulkCopy = new SqlBulkCopy(AcesCxn))
                {
                    var wheelAttrs = new List<string>
                    {
                        "YearID", "MakeID", "ModelID", "SubModelID", "DriveTypeID",
                        "BodyTypeID", "RegionID", "ThreadSize", "PartType", "PartNumber",
                        "FitmentPosition", "FitmentType", "Brand", "Model", "Finish",
                        "WheelSize", "BoltPattern"
                    };
                    var tireAttrs = new List<string>
                    {
                        "YearID", "MakeID", "ModelID", "SubModelID", "DriveTypeID", 
                        "BodyTypeID", "RegionID", "PartType", "PartNumber", "Brand",
                        "FitmentPosition", "FitmentType",
                    };
                    ObjectReader objReader = null;
                    var records = csv.GetRecords<object>();
                    switch (tireOrWheel)
                    {
                        case "wheel":
                            foreach(var attr in wheelAttrs)
                            {
                                bulkCopy.ColumnMappings.Add(attr, attr);
                            }

                            objReader = ObjectReader.Create(records, wheelAttrs.ToArray());
                            break;
                        case "tire":
                            foreach(var attr in tireAttrs)
                            {
                                bulkCopy.ColumnMappings.Add(attr, attr);
                            }
                            objReader = ObjectReader.Create(records, tireAttrs.ToArray());
                            break;
                    }
                    bulkCopy.DestinationTableName = tableName; // This is where the data is going to be inserted
                    bulkCopy.BulkCopyTimeout = 2000;
                    bulkCopy.WriteToServer(objReader);
                }
            }
            catch (Exception e)
            {
                WriteLog("#error: " + e.Message + "\n#stack trace: " + e.StackTrace);
                Console.WriteLine("Stop for exception.");
                Console.ReadKey();
            }

        }
    }
}
