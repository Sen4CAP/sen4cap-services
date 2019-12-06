# Sen4CAP Services
Sen2Agri Extensions for Sen4CAP

## How to build
JDK 8 or JRE 8 are required to build and run the services.
Before building and installing them, proceed with the build of Sen2Agri Services.

Clone this repository:
```
git clone https://github.com/Sen4CAP/sen4cap-services.git
```
Enter the folder into which the cloning was performed and issue:
```
mvn clean install
```
Assuming that the folder for these services is is:
```
~/code/sen4cap-services
```
then, in the subfolder
```
~/code/sen4agri-services/sen4cap-services-kit/target/sen4cap-services-<version>
```
will contain the executables of the services (the initial commit version is 2.0.3). Follow the Sen4CAP user manual to configure the database connection and integrate them into the Sen4CAP system.
