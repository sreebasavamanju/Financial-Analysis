# Nifty Analysis Using Spark


After Java 17 , we need to pass below arguments to java commad when running the java main class, otherwise error will throw because of illegal reflective access 
```
--add-opens=java.base/java.lang=ALL-UNNAMED 
--add-opens=java.base/java.lang.invoke=ALL-UNNAMED 
--add-opens=java.base/java.lang.reflect=ALL-UNNAMED 
--add-opens=java.base/java.io=ALL-UNNAMED 
--add-opens=java.base/java.net=ALL-UNNAMED 
--add-opens=java.base/java.nio=ALL-UNNAMED 
--add-opens=java.base/java.util=ALL-UNNAMED 
--add-opens=java.base/java.util.concurrent=ALL-UNNAMED 
--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED 
--add-opens=java.base/sun.nio.ch=ALL-UNNAMED 
--add-opens=java.base/sun.nio.cs=ALL-UNNAMED 
--add-opens=java.base/sun.security.action=ALL-UNNAMED 
--add-opens=java.base/sun.util.calendar=ALL-UNNAMED 
--add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED 
```

if your using eclipse, then add above lines to VM arguments option
