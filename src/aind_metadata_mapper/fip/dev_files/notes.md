Notes:
* Need to register my mapper w/ schema. Follow up with Dan on how to do that
* make sure the protocol mapping method is sane. Dan suggests adding a column in the smartsheet
* Need to get ethics ID from extracted metadata (or somewhere else)
* calibrations from extracted metadata need to be brought in. Upgrader has calibration mapper. May be able to use this?
* I should be getting device names from the "name" field. This is a problem because the device names are repeated
* remove rig from active devices
* I need the list of active devices in the instrument to match the active devices here
* fibers should be in active devices since they are implanted devices
* need compression info in the cameras. Check with bruno/tiffany. Might be fixed?

Need from Bruno
* acquisition type should not be FIP. We want Bruno to pass this. Don't set it in constants
* notes should come from extracted model if we want them. Are there notes the experiementer can enter?
* Need device names in extracted metadata to match the names in instrument.json
* I need a "code" object from bruno. Should be in extracted metadata
