# Arjun and I made good progress on 2/4/25 but there are some additional steps and open questions:

## Next Steps
* Verify that the correct data is being extracted from the data files. We can verify this by ensuring that the stimulus epochs that are being created match those that were created by Kenta for the same dataset. We'll still need to verity that this actually conforms to the schema and is logically correct. Follow up with David and Saskia first to get a better understanding of what's expected to be in the stimulus epochs list.
* We may need to feed in the full behavior schema to the ETL process to make sure the objects conform to the schema. We already have a Session object created so maybe it's complete. 
* We need to create a simple session joiner to join with the fiber photometry session. 
* We need to verify that all information that was being added to the session files by Kenta is being added to the combined session objects. 
* We need to properly extract the session start and end times from the data files. In the fiber photometry session, the session start time is currently just an argument in models.py. We'll need to dig into the harp files to get the correct session start and end times. 

## Questions