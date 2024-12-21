# CSCI3170-project

# Desciption

This project is CSCI3170 project, which is a computer part sales system.

# Database structure

category(cID: integer, cName: string) \
manufacturer(mID: integer, mName: string, mAddress: String, mPhoneNumber: integer) \
part(pID: integer, pName:string, pPrice: integer, mID: integer, cID: integer, pWarrantyPeriod:  integer, pAvailableQuantity: integer) \
salesperson(sID: integer, sName: string, sAddress: string, sPhoneNumber: integer, sExperience: integer) \
transaction(tID: integer, pID: integer, sID: integer, tDate: date) 

# run 
Please ensure you install JDK. \
Please ensure your system is linux / macOS. \
Type following command in termial:

```
./build.sh
./run.sh
```

# Use
Type the number of choice you want \
Or type "custom" in main manu to search by custom SQL.
