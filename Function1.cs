using System;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Azure.Messaging.ServiceBus;
using Microsoft.Azure.ServiceBus.Management;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System.Text;
using System.Collections.Generic;
using System.Net.Http;
using System.Text.RegularExpressions;
using System.Configuration;
using System.Net.Http.Headers;
using System.Data;
using Dapper;
using System.Data.SqlClient;
using System.Linq;
using Microsoft.Extensions.Configuration;
using System.Threading;

namespace ServiceBusMessageReader
{

    public class ServiceBusReader
    {
        static string topicName = ConfigurationManager.AppSettings["TopicName"];
        static string subscriptionName = ConfigurationManager.AppSettings["SubscriptionName"];
        //static string topicName = "allevaentities";
        //static string subscriptionName = "allevaentitiessubscription";
        // TODO: API URLs and Endpoint --- initialize via config

        static string billingApiurl = ConfigurationManager.AppSettings["BillingApiUrl"];
        //static string billingApiurl = "https://app-abs-core-api-int-centralus-001.azurewebsites.net/api/v1/";

        // needs to be production's rest api link 
        //static string emrApiUrl = "http://rest-api.allevasoft.local:5000/v1/";
        public static IConfiguration _staticConfiguration;
        public readonly IConfiguration _configuration;
        public ServiceBusReader(IConfiguration configuration)
        {
            _configuration = configuration;
            _staticConfiguration = configuration;
        }

        static string authUrl = ConfigurationManager.AppSettings["AuthUrl"];

        //static string integrationURL = "https://app-abs-core-api-int-centralus-001.azurewebsites.net/api/v1/";
        //static string stagingURL = "https://stg-abs-core-api.azurewebsites.net/api/v1/";
        //static string localhostURL = "http://localhost:5000/api/v1/";
        //static string connectionString = "Endpoint=sb://stg-abs-bus.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=c5z5Bed6cXblyBr6vzsbKwTzCDroI8EyY7g0X+v0q+s="; // staging ABS servicebus connection string
        public static string connectionString = ConfigurationManager.AppSettings["ServiceBusConnectionString"];

        
        static ServiceBusClientOptions clientOptions = new ServiceBusClientOptions
        {
            TransportType = ServiceBusTransportType.AmqpWebSockets
        };
        static ServiceBusClient client;
        static ServiceBusProcessor processor;
        static List<Mapping> orgMapping;
        static List<RehabInfo> rehabMapping;

        [FunctionName("Function1")]
        public async Task Run([ServiceBusTrigger("allevaentities", "AllevaEntitiesSubscription", Connection = "ServiceBusConnectionString")] string mySbMsg)
        {
            Console.WriteLine("triggered");
            topicName = _configuration["TopicName"];
            subscriptionName = _configuration["SubscriptionName"];
            billingApiurl = _configuration["BillingApiUrl"];
            authUrl = _configuration["AuthUrl"];
            connectionString = _configuration["ServiceBusConnectionString"];

            client = new(connectionString, clientOptions);

            ManagementClient managementClient = new ManagementClient(connectionString);


            var runTimeInfo = await managementClient.GetSubscriptionRuntimeInfoAsync(topicName, subscriptionName);
            orgMapping = await getOrgMapping();
            var messageCount = runTimeInfo.MessageCountDetails.ActiveMessageCount;
            if (messageCount >= 1)
            {
                var processorOptions = new ServiceBusProcessorOptions
                {
                    AutoCompleteMessages = false,
                    MaxConcurrentCalls = 2
                };
                processor = client.CreateProcessor(topicName, subscriptionName, processorOptions);

                try
                {
                    // add handler to process messages
                    processor.ProcessMessageAsync += MessageHandler;

                    // add handler to process any errors
                    processor.ProcessErrorAsync += ErrorHandler;

                    // start processing 
                    await processor.StartProcessingAsync();

                    //Console.WriteLine("Wait for a minute and then press any key to end the processing");
                    //Console.ReadKey();
                }
                finally
                {
                    //await processor.DisposeAsync();
                    // Calling DisposeAsync on client types is required to ensure that network
                    // resources and other unmanaged objects are properly cleaned up.
                }
            }
        }

        static async Task<List<Mapping>> getOrgMapping()
        {
            HttpResponseMessage response = await new HttpClient().GetAsync(billingApiurl + "getemrtobillingorgmapping");
            //HttpResponseMessage response = await new HttpClient().GetAsync("http://localhost:5000/api/v1/getemrtobillingorgmapping");

            string responseBody = await response.Content.ReadAsStringAsync();
            var mapping = JObject.Parse(responseBody).GetValue("emrToBillingMapping").ToString();
            List<Mapping> messages = JsonConvert.DeserializeObject<List<Mapping>>(mapping);
            return messages;
        }

        static async Task<List<RehabInfo>> getRehabMapping(string tenant, string rehab)
        {
            HttpRequestMessage httpRequestMessage = new HttpRequestMessage
            {
                Method = HttpMethod.Get,
                RequestUri = new Uri(billingApiurl + "rehabs/getAll"),
                Headers = {
                    { "Tenant",tenant},
                    { "Rehab", rehab}
                }
            };
            var response = new HttpClient().SendAsync(httpRequestMessage).Result;
            //HttpResponseMessage response = await new HttpClient().GetAsync("http://localhost:5000/api/v1/rehabs/getAll");

            string responseBody = await response.Content.ReadAsStringAsync();

            List<RehabInfo> messages = JsonConvert.DeserializeObject<List<RehabInfo>>(responseBody);
            return messages;
        }

        // Handler of any errors when receiving messages
        static Task ErrorHandler(ProcessErrorEventArgs args)
        {
            Console.WriteLine(args.Exception.ToString());
            return Task.CompletedTask;
        }

        static async Task MessageHandler(ProcessMessageEventArgs args)
        {

            string body = args.Message.Body.ToString();
            var json = JObject.Parse(body);
            var contentType = json.GetValue("ContentType");
            var source = json.GetValue("Source");
            var content = json.GetValue("Content").ToString();
            var action = json.GetValue("Action");

            if (source.ToString()=="EXT_ALLEVA_SERVICE")
            {
                await CallBillingAPI(content, contentType, source, action, args);
            }
            else if (source.ToString() == "ABS")
            {
                await CallEMRAPI(content, contentType, source, action, args);
            }

            return;
        }

        static async Task CallBillingAPI(string content, JToken contentType, JToken source, JToken action,ProcessMessageEventArgs args)
        {
            JObject jsonContent = JObject.Parse(content);

            if (contentType != null && (string)contentType == "Client")
            {
                dynamic billingPatient = new System.Dynamic.ExpandoObject();
                billingPatient.EMRLinkSequenceId = (string)jsonContent.GetValue("EMRLinkSequenceId");
                billingPatient.EMRLinkId = (string)jsonContent.GetValue("EMRLinkId");
                billingPatient.EMRFacility = (string)jsonContent.GetValue("FacilityId");
                billingPatient.FirstName = (string)jsonContent.GetValue("FirstName");
                billingPatient.MiddleName = (string)jsonContent.GetValue("MiddleName") ?? null;
                billingPatient.LastName = (string)jsonContent.GetValue("LastName");
                billingPatient.DOB = (DateTime)jsonContent.GetValue("DateOfBirth");

                billingPatient.ExtendedGender = (string)jsonContent.GetValue("Gender");
                billingPatient.Sex = (string)jsonContent.GetValue("Gender");
                if (billingPatient.Sex == "Male") billingPatient.Sex = "M";
                else if (billingPatient.Sex == "Female") billingPatient.Sex = "F";
                else billingPatient.Sex = "U";

                billingPatient.Address = (string)jsonContent.GetValue("Address") ?? "";
                billingPatient.Address2 = (string)jsonContent.GetValue("Address2") ?? null;
                billingPatient.City = (string)jsonContent.GetValue("City") ?? "";
                billingPatient.State = ((string)jsonContent.GetValue("State") + "               ")[..10];


                billingPatient.Zip = (string)jsonContent.GetValue("Postal") ?? "";
                billingPatient.Country = (string)jsonContent.GetValue("Country") ?? "";


                billingPatient.Email = (string)jsonContent.GetValue("Email");
                var phone = jsonContent.GetValue("Home").ToString();
                JObject jsonPhone = JObject.Parse(phone);
                billingPatient.HomePhone = (string)jsonPhone.GetValue("number") ?? null;
                if (billingPatient.HomePhone != null) billingPatient.HomePhone = Regex.Replace(jsonContent.GetValue("Home").ToString(), "[^0-9]", "");

                phone = jsonContent.GetValue("Office").ToString();
                jsonPhone = JObject.Parse(phone);
                billingPatient.OfficePhone = (string)jsonPhone.GetValue("number") ?? null;
                if (billingPatient.OfficePhone != null) billingPatient.OfficePhone = Regex.Replace(jsonContent.GetValue("Office").ToString(), "[^0-9]", "");

                phone = jsonContent.GetValue("Cell").ToString();
                jsonPhone = JObject.Parse(phone);
                billingPatient.MobilePhone = (string)jsonPhone.GetValue("number") ?? null;
                if (billingPatient.MobilePhone != null) billingPatient.MobilePhone = Regex.Replace(jsonContent.GetValue("Cell").ToString(), "[^0-9]", "");

                billingPatient.RehabId = jsonContent.GetValue("RehabFacilityId").ToString();
                billingPatient.Source = source;

                string jsonString = JsonConvert.SerializeObject(billingPatient);

                string organizationId = jsonContent.GetValue("OrganizationId").ToString();
                string rehabId = jsonContent.GetValue("RehabFacilityId").ToString();

                string tenant = "Rish";
                string rehab = "Rehab2";
                foreach (Mapping map in orgMapping)
                {
                    map.SourceOrgId = map.SourceOrgId.ToUpper();
                    organizationId = organizationId.ToUpper();
                    if (map.SourceOrgId == organizationId)
                    {
                        tenant = map.DestinationOrg;
                    }
                }
                rehabMapping = await getRehabMapping(tenant, rehab);

                foreach (var potentialRehab in rehabMapping)
                {
                    if (potentialRehab.emrLinkSequenceId == rehabId)
                    {
                        rehab = potentialRehab.rehubId;
                    }
                }
                HttpClient httpClient = new HttpClient();

                if ((string)action == "Client Create")
                {
                    jsonString = jsonString.Replace("/", "");

                    HttpRequestMessage httpRequestMessage = new HttpRequestMessage
                    {
                        Method = HttpMethod.Post,
                        RequestUri = new Uri(billingApiurl + "patients/createallevapatient"),
                        Headers = {
                        { "Tenant",tenant},
                        { "Rehab", rehab}
                        },
                        Content = new StringContent(jsonString, Encoding.UTF8, "application/json")
                    };
                    var response = httpClient.SendAsync(httpRequestMessage).Result;
                    if (response.IsSuccessStatusCode)
                    {
                        // complete the message. message is deleted from the subscription. 
                        await args.CompleteMessageAsync(args.Message);
                    }
                }
                else if ((string)action == "Client Update")
                {

                    rehabMapping = await getRehabMapping(tenant, rehab);

                    HttpRequestMessage httpRequestMessage = new HttpRequestMessage
                    {
                        Method = HttpMethod.Post,
                        RequestUri = new Uri(billingApiurl + "updatepatientbyemrid"),
                        Headers = {
                            { "Tenant",tenant},
                            { "Rehab", rehab}
                            },
                        Content = new StringContent(jsonString, Encoding.UTF8, "application/json")
                    };
                    var response = httpClient.SendAsync(httpRequestMessage).Result;
                    // If response is not 200 OK, put the message back in topic
                    if (response.IsSuccessStatusCode)
                    {
                        // complete the message. message is deleted from the subscription. 
                        await args.CompleteMessageAsync(args.Message);
                        // stop processing 
                    }
                }
            }
            else if (contentType != null && (string)contentType == "Insurance")
            { }
            return;
        }

        public static IDbConnection MasterConnection
        {
            get
            {
                string connectionString;
                string pw;

                // connection string will be a env variable, for production master database
                // TODO: mast db connection string and password from config
                connectionString = _staticConfiguration["EmrDbConnectionString"];
                pw = _staticConfiguration["EmrDbPassword"];

                SqlConnectionStringBuilder conn = new SqlConnectionStringBuilder(connectionString)
                {
                    Password = pw
                };
                return new SqlConnection(conn.ConnectionString);
            }
        }

        static async Task CallEMRAPI(string content, JToken contentType, JToken source, JToken action, ProcessMessageEventArgs args)
        {
            Console.WriteLine("in EMR API");
            // log into EMR API, get authentication token
            HttpClient httpClient = new HttpClient();
            JObject jsonContent = JObject.Parse(content);
            var rehabId = jsonContent.GetValue("RehabFacilityId").ToString();
            string domainName = "";
            string rehabName = "";
            //rehabId = "54";

            using (IDbConnection conn = MasterConnection)
            {
                string sqlCommand = $@"select DomainName from RehabCompanies where rehabid=@rehabId";

                var result = await conn.QueryAsync<string>(sqlCommand, new {rehabId});
                domainName = result.FirstOrDefault();

                sqlCommand = $@"select RehabName from RehabCompanies where rehabid=@rehabId";

                result = await conn.QueryAsync<string>(sqlCommand, new { rehabId });
                rehabName = result.FirstOrDefault();
            }


            // TODO: Service Account and adding log info for the user that request/created the update. Pending on IdentityServer, SSO and auth-v2
            //var jsonString = "{\r\n  \"username\": \"jramos\",\r\n  \"password\": \"Test@1234\",\r\n  \"rehabName\": \""+rehabName+"\",\r\n  \"domainName\": \""+domainName+"\"\r\n}";
            // TODO: username and password from config
            var jsonString = "{\r\n  \"username\": \""+ _staticConfiguration["AuthUsername"] + "\",\r\n  \"password\": \""+ _staticConfiguration["AuthPassword"] +"\",\r\n  \"rehabName\": \"" + rehabName + "\",\r\n  \"domainName\": \"" + domainName + "\"\r\n}";
            HttpRequestMessage httpRequestMessage = new HttpRequestMessage
            {
                Method = HttpMethod.Post,
                RequestUri = new Uri(authUrl),
                Content = new StringContent(jsonString, Encoding.UTF8, "application/json")
            };
            var response = httpClient.SendAsync(httpRequestMessage).Result;
            string body = await response.Content.ReadAsStringAsync();
            var token = JObject.Parse(body).GetValue("token").ToString();
            httpClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", token);
            httpClient.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
            // added token and default media type


            dynamic emrPatientFields = new System.Dynamic.ExpandoObject();
            emrPatientFields.EMRLinkSequenceId = (string)jsonContent.GetValue("EMRLinkSequenceId");
            emrPatientFields.EMRLinkId = (string)jsonContent.GetValue("EMRLinkId");
            emrPatientFields.EMRFacility = (string)jsonContent.GetValue("FacilityId");
            emrPatientFields.FirstName = (string)jsonContent.GetValue("FirstName");
            emrPatientFields.MiddleName = (string)jsonContent.GetValue("MiddleName") ?? null;
            emrPatientFields.LastName = (string)jsonContent.GetValue("LastName");
            emrPatientFields.DOB = (DateTime)jsonContent.GetValue("DateOfBirth");

            
            emrPatientFields.ExtendedGender = (string)jsonContent.GetValue("Gender");
            if (emrPatientFields.ExtendedGender == "M") emrPatientFields.ExtendedGender = "Male";
            if (emrPatientFields.ExtendedGender == "F") emrPatientFields.ExtendedGender = "Female";

            emrPatientFields.Address = (string)jsonContent.GetValue("Address") ?? "";
            emrPatientFields.Address2 = (string)jsonContent.GetValue("Address2") ?? null;
            emrPatientFields.City = (string)jsonContent.GetValue("City") ?? "";
            emrPatientFields.State = ((string)jsonContent.GetValue("State") + "               ")[..10];


            emrPatientFields.Zip = (string)jsonContent.GetValue("Postal") ?? null;
            emrPatientFields.Country = (int)jsonContent.GetValue("Country");

            emrPatientFields.Email = (string)jsonContent.GetValue("Email");

            var phone = jsonContent.GetValue("Home").ToString();
            JObject jsonPhone = JObject.Parse(phone);
            if ((string)jsonPhone.GetValue("number") != "") emrPatientFields.HomePhone = String.Format("{0:(###) ###-####}", Convert.ToInt64((string)jsonPhone.GetValue("number")));
            else emrPatientFields.HomePhone = "";

            phone = jsonContent.GetValue("Office").ToString();
            jsonPhone = JObject.Parse(phone);
            if ((string)jsonPhone.GetValue("number") != "") emrPatientFields.OfficePhone = String.Format("{0:(###) ###-####}", Convert.ToInt64((string)jsonPhone.GetValue("number")));
            else emrPatientFields.OfficePhone = "";

            phone = jsonContent.GetValue("Cell").ToString();
            jsonPhone = JObject.Parse(phone);
            if ((string)jsonPhone.GetValue("number") != "") emrPatientFields.MobilePhone = String.Format("{0:(###) ###-####}", Convert.ToInt64((string)jsonPhone.GetValue("number")));
            else emrPatientFields.MobilePhone = "";

            jsonString = JsonConvert.SerializeObject(emrPatientFields);

            string emrApiUrl = _staticConfiguration[rehabId.ToString()];

            httpRequestMessage = new HttpRequestMessage
            {
                Method = HttpMethod.Patch,
                RequestUri = new Uri(emrApiUrl + "clients/" + emrPatientFields.EMRLinkId.ToString()),
                //RequestUri = new Uri(emrApiUrl + "clients/" + 1006),
                Content = new StringContent(jsonString, Encoding.UTF8, "application/json")
            };

            response = httpClient.SendAsync(httpRequestMessage).Result;
            Console.WriteLine(response);
            if (response.IsSuccessStatusCode)
            {
                // complete the message. message is deleted from the subscription. 
                await args.CompleteMessageAsync(args.Message);
                // stop processing 
            }
            //else
            //{
            //    await args.RenewMessageLockAsync(args.Message);
            //}
        }
    }


    public class Mapping
    {
        public string SourceOrgId { get; set; }
        public string DestinationOrg { get; set; }
        public string SourceRehab { get; set; }
        public string DestinationRehab { get; set; }
    }

    public class RehabInfo
    {
        public string rehubId { get; set; }
        public string id { get; set; }
        public string name { get; set; }
        public string address { get; set; }
        public string address2 { get; set; }
        public string city { get; set; }
        public string state { get; set; }
        public string zip { get; set; }
        public string country { get; set; }
        public string taxId { get; set; }
        public string emrLinkSequenceId { get; set; }
    }
}
