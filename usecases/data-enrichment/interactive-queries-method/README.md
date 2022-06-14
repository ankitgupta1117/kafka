# Interactive Queries Method Enrichment Example

In this method we will try to enrich country name in products by querying from state store. 

The state stores are stored by each of the Stream client Instance. 

On the top of the Stream Instances a REST API layer is implemented.
This REST layer will expose local state store of each stream instance over REST.
API layer query the local state store for the country code. If its found in local store then value is returned, else
a REST api call is made to the particular instance containing the key-value.

