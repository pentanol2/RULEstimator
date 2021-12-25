<h1>RUL Estimator Using Apache Spark</h1>
<p>In this project the main gold is to provide a general prototype
for a small CPS. We are showcasing the example of a system
that predicts the remaining useful life (RUL) of a set of running engines.
The job is performed through three modules:</p>
<ul>
    <li>ML Module:<p>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;In machine learning module we perform not only the modeling
job done in ml, but we perform data collection, eda, and preprocessing job.
After validation and testing, the final output of this module is a machine learning
model to deploy to work for us in real time.
</p></li>
<br>
    <li>RT Module:<p>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;RT or Real time Analysis Module. In this module the real time analysis module, uses the last updated/deployed model
to make prediction on sensor signals incoming from the  MQTT broker.
The prediction result is logged to text files, each corresponding to an analysis session.
</p></li>
<br>
    <li>MQTT Client Broker Module:<p>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;The MQTT module is a simulation
of sensor behavior in real life. The MQTT client reads sensor data for a dataset file
line by line in pre-specified time intervals and sends the sensors
reading to the MQTT broker. The MQTT broker itself forwards
the sensor data to Spark's Streaming connector specified for MQTT.
</p></li>
</ul>