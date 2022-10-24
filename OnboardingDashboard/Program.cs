using Amazon;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DocumentModel;
using Amazon.DynamoDBv2.Model;
using Amazon.Runtime.CredentialManagement;
using Amazon.SimpleSystemsManagement;
using Amazon.SimpleSystemsManagement.Model;
using CsvHelper;
using Newtonsoft.Json;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace OnboardingDashboard
{
    internal class Program
    {
        private static async Task Main(string[] args)
        {
            Console.Title = "Investment";
            Console.Write("Insira o profile: ");
            string profileName = Console.ReadLine();

            SharedCredentialsFile sharedFile = new SharedCredentialsFile();
            sharedFile.TryGetProfile(profileName, out CredentialProfile profile);
            AWSCredentialsFactory.TryGetAWSCredentials(profile, sharedFile, out Amazon.Runtime.AWSCredentials credentials);

            AmazonDynamoDBClient _dynamoDbClient = new AmazonDynamoDBClient(credentials, RegionEndpoint.USEast1);

            AmazonSimpleSystemsManagementClient _ssmClient = new AmazonSimpleSystemsManagementClient(credentials, RegionEndpoint.USEast1);


            Console.WriteLine(DateTime.Now);

            Console.WriteLine("Selecione uma opcao ");
            Console.WriteLine("1 - Suitability de todos os clientes");
            Console.WriteLine("2 - Suitability de clientes específico");
            Console.WriteLine("3 - Suitability de clientes específico com respostas");
            Console.WriteLine("4 - Logs aplicacao investimento");
            Console.WriteLine("5 - Atualizar dados Correção erro KINESIS");
            Console.WriteLine("6 - Suitability de clientes com respostas (TODOS OS CLIENTES DE INVESTIMENTOS)");
            Console.WriteLine("7 - Change investment customer SendWithdrawInvoice flag");
            Console.WriteLine("8 - Suitability de clientes com respostas solicitacao #1139");
            Console.WriteLine("9 - Dados de clientes para Relatorio da Anbima");
            Console.WriteLine("10 - Suitability Com perfil investidor (sem resposta) e termo");
            Console.WriteLine("11 - Todos os Suitability de todos os clientes da base");
            Console.WriteLine("0 - Fechar");

            string option = Console.ReadLine();

            switch (option)
            {
                case "1":
                    Console.WriteLine($"Carregando Suitability ...");
                    List<CustomerSuitabilityHistory> leads = await GetSuitabilityHistory(_dynamoDbClient);
                    Verify(leads, "1");
                    Console.WriteLine($"Gerando dados estatisticos...");
                    Console.WriteLine($"Será criado um arquivos no diretorio");
                    break;

                case "2":
                    await GetProfileSpecificUser(_dynamoDbClient);
                    break;

                case "3":
                    await GetFullProfileSpecificUser(_dynamoDbClient);
                    break;

                case "4":
                    await GetLogsAplicacaoUser(_dynamoDbClient);
                    break;

                case "5":
                    await CorrecaoDadosCustomer(_dynamoDbClient);
                    break;

                case "6":
                    await GetFullProfileaLLUserS(_dynamoDbClient);
                    break;

                case "7":
                    Console.WriteLine("-------------------------------------------------------------------------");
                    await UpdateInvestmentSendWithdrawInvoiceCustomerFlag(_dynamoDbClient);
                    break;

                case "8":
                    Console.WriteLine("-------------------------------------------------------------------------");
                    await CustomerSuitabilityWithAnswersSpecificUser(_dynamoDbClient);
                    break;
                case "9":
                    Console.WriteLine("-------------------------------------------------------------------------");
                    GetCustomersInfoByYear(_ssmClient);
                    break;
                case "10":
                    Console.WriteLine("-------------------------------------------------------------------------");
                    await GetCustomerSuitabilityAndTermByDate(_dynamoDbClient);
                    break;
                case "11":
                    Console.WriteLine("-------------------------------------------------------------------------");
                    await GetAllCustomerSuitabilities(_dynamoDbClient);
                    break;
            }

            //Console.WriteLine($"Carregando whitelist...");
            //var whiteList = await GetWhitelist(_dynamoDbClient);

            //Console.WriteLine($"Gerando dados estatisticos...");
            //Console.WriteLine($"Serao criados 4 arquivos no diretorio");
            //Verify(leads, whiteList);

            Console.WriteLine($"Fim");
            Console.ReadLine();
        }


        #region processo11

        internal static async Task GetAllCustomerSuitabilities(AmazonDynamoDBClient _dynamoDbClient)
        {
            List<CustomerSuitabilityHistory> customerSuitabilities = await GetSuitabilityHistory(_dynamoDbClient);

            ConcurrentBag<DatabaseSuitability> results = new ConcurrentBag<DatabaseSuitability>();

            Parallel.ForEach(
                customerSuitabilities,
                new ParallelOptions { MaxDegreeOfParallelism = Convert.ToInt32(Math.Ceiling((Environment.ProcessorCount * 0.75) * 1.0)) },
                customerSuitability =>
                {
                    int totalCustomerSuitability = customerSuitability.CustomerSuitabilities.Count();

                    for (int i = 0; i < customerSuitability.CustomerSuitabilities.Count(); i++)
                    {
                        CustomerSuitabilityHistoryAnswers item = customerSuitability.CustomerSuitabilities.ToList()[i];

                        results.Add(new DatabaseSuitability
                        {
                            CustomerId = customerSuitability.CustomerId,
                            TotalOfCustomerSuitability = totalCustomerSuitability,
                            ChangedDate = item.AnsweredAt,
                            CustomerProfile = ProfileResolver(item.Profile),
                            SuitabilityPosition = (i + 1)
                        });
                    }
                });

            int ItemsPerPage = 10000;
            int TotalItems = results.Count();
            int TotalPages = (int)Math.Ceiling((double)TotalItems / ItemsPerPage);

            Console.WriteLine($"Será criado {TotalPages} arquivos no diretorio com até {ItemsPerPage} linhas");

            for (int page = 0; page < TotalPages; page++)
            {
                IEnumerable<DatabaseSuitability> item = results.Skip(ItemsPerPage * page).Take(ItemsPerPage);
                GenerateFileToOption11(results.ToList(), $"todos_Suitability_do_cliente{(page + 1)}");
            }
        }


        internal static void GenerateFileToOption11(List<DatabaseSuitability> data, string fileName)
        {
            string[] _headers = { "Cliente", "Total de suitability respondidos", "Ordem desse de resposta para esse perfil", "Data suitability realizado", "Perfil" };

            using (MemoryStream memoryStream = new MemoryStream())
            using (StreamWriter streamWriter = new StreamWriter(memoryStream))
            using (CsvWriter csvWriter = new CsvWriter(streamWriter))
            {
                csvWriter.WriteField(_headers);
                csvWriter.NextRecord();

                foreach (DatabaseSuitability item in data)
                {
                    csvWriter.WriteField(item.CustomerId);
                    csvWriter.WriteField(item.TotalOfCustomerSuitability);
                    csvWriter.WriteField(item.SuitabilityPosition);
                    csvWriter.WriteField(item.ChangedDate.ToString("dd/MM/yyyy"));
                    csvWriter.WriteField(item.CustomerProfile);
                    csvWriter.NextRecord();
                }

                streamWriter.Flush();
                memoryStream.Position = 0;

                FileStream fileStream = new FileStream($"{fileName}.csv", FileMode.Create, FileAccess.Write);
                memoryStream.CopyTo(fileStream);
                fileStream.Dispose();
            }
        }


        #endregion




        internal static async Task GetCustomerSuitabilityAndTermByDate(AmazonDynamoDBClient _dynamoDbClient)
        {
            Console.WriteLine($"Data inicio de Corte dd/mm/yyyy -> exemplo 10/06/2022");
            string dataInicio = Console.ReadLine();
            string[] dataSeparadas = dataInicio.Replace(" ", "").Split("/");
            DateTime dataInicioFormated = new DateTime(Convert.ToInt32(dataSeparadas[2]), Convert.ToInt32(dataSeparadas[1]), Convert.ToInt32(dataSeparadas[0]));

            Console.WriteLine($"Data fim de Corte dd/mm/yyyy -> exemplo 10/06/2022");
            string dataFim = Console.ReadLine();
            string[] dataFimSeparada = dataInicio.Replace(" ", "").Split("/");
            DateTime dataFimFormated = new DateTime(Convert.ToInt32(dataSeparadas[2]), Convert.ToInt32(dataSeparadas[1]), Convert.ToInt32(dataSeparadas[0]));


            List<CustomerWithTerms> leadsList = new List<CustomerWithTerms>();
            List<string> cpfSemSuitabilityNaData = new List<string>();

            Console.WriteLine($"Gerando dados estatisticos...");

            Console.WriteLine($"PROCESSANDO........");

            List<CustomerSuitabilityHistory> allCustomerSuitabilityHistory = await GetSuitabilityHistory(_dynamoDbClient);

            foreach (CustomerSuitabilityHistory customerSuitability in allCustomerSuitabilityHistory)
            {
                IEnumerable<CustomerSuitabilityHistoryAnswers> resutlt = customerSuitability.CustomerSuitabilities.Where(x => x.AnsweredAt.Date >= dataInicioFormated.Date && x.AnsweredAt.Date <= dataFimFormated.Date);

                if (resutlt.Any())
                {
                    List<CustomerTerms> terms = await GetCustomerIvestmentTerm(customerSuitability.CustomerId, _dynamoDbClient);

                    if (terms.Any())
                    {
                        CustomerTerms customerCriTerm = terms.FirstOrDefault(x => x.Type.Equals("INVESTOR_QUALIFIED"));
                        CustomerSuitabilityHistoryAnswers isFirst = customerSuitability.CustomerSuitabilities.FirstOrDefault();

                        leadsList.Add(new CustomerWithTerms
                        {
                            CustomerId = customerSuitability.CustomerId,
                            AnsweredAt = resutlt.LastOrDefault().AnsweredAt,
                            CriTermAssign = customerCriTerm != null ? customerCriTerm.CreatedDate : new DateTime(),
                            IsFirstSuitability = isFirst.AnsweredAt.Equals(resutlt.LastOrDefault().AnsweredAt),
                            IsFirstTimeTerm = customerCriTerm != null ? true : false,
                            Profile = resutlt.LastOrDefault().Profile
                        });
                    }
                }
            }

            Console.WriteLine($"Gerando Arquivos");

            int ItemsPerPage = 1000000;
            int TotalItems = leadsList.Count();
            int TotalPages = (int)Math.Ceiling((double)TotalItems / ItemsPerPage);

            Console.WriteLine($"Será criado {TotalPages} arquivos no diretorio com até {ItemsPerPage} linhas");

            for (int page = 0; page < TotalPages; page++)
            {
                IEnumerable<CustomerWithTerms> item = leadsList.Skip(ItemsPerPage * page).Take(ItemsPerPage);
                await VerifyCustomerTerms(item.ToList(), $"Suitability_informacao_com_termo_{(page + 1)}");
            }



            //await VerifyCustomerTerms(leadsList, "");

            Console.ForegroundColor = ConsoleColor.Yellow;
            Console.WriteLine($"Processo finalizado");
            Console.ResetColor();
        }


        internal static async Task<List<CustomerTerms>> GetCustomerIvestmentTerm(string id, AmazonDynamoDBClient _dynamoDbClient)
        {
            AttributeValue hashKey = new AttributeValue { S = id };

            Dictionary<string, Condition> keyConditions = new Dictionary<string, Condition>
            {
                { "Id", new Condition { ComparisonOperator = ComparisonOperator.EQ, AttributeValueList = new List<AttributeValue> { hashKey } } },
            };

            return await QueryCustomerTermsByCustomerId(keyConditions, _dynamoDbClient, "Barigui.Services.Terms_CustomerTerms");
        }


        internal static async Task<List<CustomerTerms>> QueryCustomerTermsByCustomerId(Dictionary<string, Condition> keyConditions, AmazonDynamoDBClient _dynamoDbClient, string tableName)
        {
            Dictionary<string, AttributeValue> lastKeyEvaluated = null;
            List<CustomerTerms> result = new List<CustomerTerms>();

            do
            {
                QueryRequest request = new QueryRequest
                {
                    TableName = tableName,
                    ExclusiveStartKey = lastKeyEvaluated,
                    KeyConditions = keyConditions,
                };

                QueryResponse response = await _dynamoDbClient.QueryAsync(request);

                foreach (Dictionary<string, AttributeValue> item in response.Items)
                {
                    Document document = Amazon.DynamoDBv2.DocumentModel.Document.FromAttributeMap(item);
                    result.Add(JsonConvert.DeserializeObject<CustomerTerms>(document.ToJson()));
                }

                lastKeyEvaluated = response.LastEvaluatedKey;

            } while (lastKeyEvaluated != null && lastKeyEvaluated.Count > 0);

            return result;
        }

        internal static async Task VerifyCustomerTerms(List<CustomerWithTerms> data, string fileName)
        {
            string[] _headers = { "Cliente", "Perfil", "Data suitability", "Primeiro suitability", "Primeira assinatura do termo", "Data assinitura do termo" };

            using (MemoryStream memoryStream = new MemoryStream())
            using (StreamWriter streamWriter = new StreamWriter(memoryStream, System.Text.Encoding.UTF8))
            using (CsvWriter csvWriter = new CsvWriter(streamWriter))
            {
                csvWriter.WriteField(_headers);
                csvWriter.NextRecord();

                foreach (CustomerWithTerms item in data)
                {
                    csvWriter.WriteField(item.CustomerId);
                    csvWriter.WriteField(item.AnsweredAt.ToString("dd/MM/yyyy"));
                    csvWriter.WriteField(ProfileResolver(item.Profile));
                    csvWriter.WriteField(item.IsFirstSuitability);
                    csvWriter.WriteField(item.IsFirstTimeTerm);
                    csvWriter.WriteField(item.CriTermAssign.ToString("dd/MM/yyyy"));
                    csvWriter.NextRecord();
                }

                streamWriter.Flush();
                memoryStream.Position = 0;

                FileStream fileStream = new FileStream($"{fileName}.csv", FileMode.Create, FileAccess.Write);
                memoryStream.CopyTo(fileStream);
                fileStream.Dispose();
            }
        }





        internal static bool GetCustomersInfoByYear(AmazonSimpleSystemsManagementClient ssmClient)
        {
            Console.Write("Insira o ano de referencia: ");
            string anoReferencia = Console.ReadLine();

            GetParameterRequest request = new GetParameterRequest()
            {
                Name = "/Barigui/Lydians/ConnectionString",
                WithDecryption = true
            };
            GetParameterResponse response = ssmClient.GetParameterAsync(request).GetAwaiter().GetResult();

            string _connectionParameterStore = response.Parameter.Value;
            using (SqlConnection db = new SqlConnection(_connectionParameterStore))
            {
                string strSql = $@"BEGIN
	                            DECLARE @vEmpresa CHAR(2)
	                            DECLARE @vAgencia CHAR(4)
	                            DECLARE @vData DATETIME
	
                            -- Alterar valores somente aqui
	                            SET @vEmpresa = '01'
	                            SET @vAgencia = '0001'
	                            SET @vData  = YEAR('{anoReferencia}-12-31') --> OBS: Alterar somente o ano 
                            --------------------------------------					
				
	                            SELECT RNFTIT.CODCLIENTE AS CPF_CNPJ
		                            ,(
			                            CASE 
				                            WHEN EXISTS (
						                            SELECT DISTINCT CODOPER
						                            FROM RNFTITLANC WITH (NOLOCK)
						                            WHERE EMPRESA = @vEmpresa
							                            AND CODCLIENTE = RNFTIT.CODCLIENTE
							                            AND CODOPER IN (
								                            1
								                            ,2
								                            ,3
								                            ,4
								                            )
							                            AND YEAR(DTLANC) = @vData 
						                            )
					                            THEN 'S'
				                            ELSE 'N'
				                            END
			                            ) AS MOVIMENTACOES
		                            ,(
			                            SELECT MIN(DTEMISSAO)
			                            FROM RNFTITULOS WITH (NOLOCK)
			                            WHERE EMPRESA = @vEmpresa
				                            AND AGENCIA = @vAgencia
				                            AND CODCLIENTE = RNFTIT.CODCLIENTE
			                            ) AS PRIMEIRA_APLICACAO
		                            ,(
			                            CASE 
				                            WHEN EXISTS (
						                            SELECT DTSITUACAO
						                            FROM RNFTITULOS WITH (NOLOCK)
						                            WHERE EMPRESA = @vEmpresa
							                            AND AGENCIA = @vAgencia
							                            AND DAY(DTSITUACAO) = 31
							                            AND MONTH(DTSITUACAO) = 12
							                            AND YEAR(DTSITUACAO) = @vData 
							                            AND CODCLIENTE = RNFTIT.CODCLIENTE
						                            )
					                            THEN 'S'
				                            ELSE 'N'
				                            END
			                            ) AS TITULOS_ATIVOS
	                            FROM RNFTITULOS RNFTIT WITH (NOLOCK)
	                            WHERE RNFTIT.EMPRESA = @vEmpresa
		                            AND RNFTIT.AGENCIA = @vAgencia
	                            GROUP BY RNFTIT.CODCLIENTE
	                            ORDER BY RNFTIT.CODCLIENTE

                            END";
                try
                {
                    List<MovimentoUsuario> movimentoUsuario = new List<MovimentoUsuario>();
                    using (SqlCommand command = new SqlCommand(strSql, db))
                    {
                        db.Open();
                        using (SqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                movimentoUsuario.Add(new MovimentoUsuario() { CPF_CNPJ = reader.GetString(0), MOVIMENTACOES = reader.GetString(1), PRIMEIRA_APLICACAO = reader.GetDateTime(2), TITULOS_ATIVOS = reader.GetString(3) });
                            }
                        }
                    }
                    db.Close();

                    GenerateFile(movimentoUsuario, "clientes_movimentacao_listagem");

                }
                catch (Exception ex)
                {

                    throw new Exception($"Erro ao consultar o banco de dados conforme, {ex}");
                }
            }
            return true;
        }

        internal static async Task CustomerSuitabilityWithAnswersSpecificUser(AmazonDynamoDBClient _dynamoDbClient)
        {

            Console.WriteLine($"digite os cpfs divididos por virgula e sem espaco (99999999999,88888888888,....)");
            string cpfs = Console.ReadLine();
            string[] cpfList = cpfs.Replace(" ", "").Split(",");

            List<CustomerSuitabilityHistory> leadsList = new List<CustomerSuitabilityHistory>();
            List<string> cpfSemSuitabilityNaData = new List<string>();

            Console.WriteLine($"PROCESSANDO........");

            foreach (string cpf in cpfList)
            {
                List<CustomerSuitabilityHistory> allCustomerSuitabilityHistory = await GetCustomerSuitabilityHistoryByCustomerId(_dynamoDbClient, cpf);

                if (allCustomerSuitabilityHistory.Any())
                {
                    IEnumerable<CustomerSuitabilityHistory> customerSuitabilityHistory = allCustomerSuitabilityHistory.Where(x => x.CustomerSuitabilities.Where(y => y.AnsweredAt.Year == 2020).Any());

                    if (customerSuitabilityHistory.Any())
                    {
                        CustomerSuitabilityHistory firtsSuitability = customerSuitabilityHistory.FirstOrDefault();

                        firtsSuitability.CustomerSuitabilities = new List<CustomerSuitabilityHistoryAnswers>
                        {
                            firtsSuitability.CustomerSuitabilities.OrderBy(x => x.AnsweredAt).LastOrDefault()
                        };

                        leadsList.Add(firtsSuitability);
                    }
                    else
                    {
                        cpfSemSuitabilityNaData.Add(cpf);
                    }
                }
                else
                {
                    cpfSemSuitabilityNaData.Add(cpf);
                }
            }

            Console.WriteLine($"Gerando Arquivos");

            await VerifyAllSuitabilityHistory(leadsList, "0", _dynamoDbClient);

            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine($"ATENÇÃO");
            Console.WriteLine($"Não foram encontradas respostas em 2020 para os seguintes clientes");
            Console.ResetColor();

            foreach (string item in cpfSemSuitabilityNaData)
            {
                Console.WriteLine(item);
            }

            Console.ForegroundColor = ConsoleColor.Yellow;
            Console.WriteLine($"Processo finalizado");
            Console.ResetColor();

        }

        internal static async Task<List<CustomerSuitabilityHistory>> GetCustomerSuitabilityHistoryByCustomerId(AmazonDynamoDBClient _dynamoDbClient, string customerId)
        {
            Dictionary<string, AttributeValue> lastKeyEvaluated = null;

            List<CustomerSuitabilityHistory> leads = new List<CustomerSuitabilityHistory>();

            do
            {
                ScanRequest request = new ScanRequest
                {
                    TableName = "Barigui.Services.Investment_CustomerSuitabilityHistory",
                    FilterExpression = "CustomerId = :customerId",
                    ExpressionAttributeValues = new Dictionary<string, AttributeValue>
                    {
                        {":customerId", new AttributeValue {S = customerId}}
                    },
                    Limit = 100,
                    ExclusiveStartKey = lastKeyEvaluated,
                };

                ScanResponse response = await _dynamoDbClient.ScanAsync(request);

                foreach (Dictionary<string, AttributeValue> item in response.Items)
                {
                    Document document = Amazon.DynamoDBv2.DocumentModel.Document.FromAttributeMap(item);
                    leads.Add(JsonConvert.DeserializeObject<CustomerSuitabilityHistory>(document.ToJson()));
                }

                lastKeyEvaluated = response.LastEvaluatedKey;

            } while (lastKeyEvaluated != null && lastKeyEvaluated.Count != 0);

            return leads;
        }


        internal static async Task UpdateInvestmentSendWithdrawInvoiceCustomerFlag(AmazonDynamoDBClient _dynamoDbClient)
        {
            Console.WriteLine($"Buscando clientes com suitability..... isso pode levar alguns minutos");
            List<DatabaseSuitability> allCustomerSuitability = await GetCustomerSuitabilities(_dynamoDbClient);

            Console.WriteLine($"Clientes com suitability encontrados");

            Console.WriteLine($"Atualizando WithdrawInvoiceCustomer flag..... isso pode levar alguns minutos");

            Console.Write($"processando ");

            ConsoleSpiner spin = new ConsoleSpiner();

            int cont = 0;

            foreach (DatabaseSuitability customerSuitability in allCustomerSuitability)
            {
                InvestmentCustomer investmentCustomer = await GetInvestmentCustomer(_dynamoDbClient, customerSuitability.CustomerId);

                if (investmentCustomer != null)
                {
                    spin.Turn();
                    cont++;
                    investmentCustomer.SendWithdrawInvoice = true;

                    await UpdateInvestmentCustomer(_dynamoDbClient, investmentCustomer);
                }
            }
            Console.SetCursorPosition(Console.CursorLeft - 12, Console.CursorTop);

            Console.ForegroundColor = ConsoleColor.Yellow;
            Console.WriteLine($"Processo finalizado. Foram atualizados {cont} flags");
            Console.ResetColor();
        }


        public class ConsoleSpiner
        {
            private int counter;
            public ConsoleSpiner()
            {
                counter = 0;
            }
            public void Turn()
            {
                counter++;
                switch (counter % 4)
                {
                    case 0: Console.Write("/"); break;
                    case 1: Console.Write("-"); break;
                    case 2: Console.Write("\\"); break;
                    case 3: Console.Write("|"); break;
                }
                Console.SetCursorPosition(Console.CursorLeft - 1, Console.CursorTop);
            }
        }


        internal static async Task<InvestmentCustomer> GetInvestmentCustomer(AmazonDynamoDBClient _dynamoDbClient, string customerId)
        {
            ScanFilter filter = new ScanFilter();

            filter.AddCondition("CustomerId", ScanOperator.Equal, customerId);

            ScanRequest request = new ScanRequest
            {
                TableName = "Barigui.Services.Investment_Customer",
                ScanFilter = filter.ToConditions(),
            };

            ScanResponse response = await _dynamoDbClient.ScanAsync(request);

            if (!response.Items.Any())
            {
                return null;
            }

            Document document = Amazon.DynamoDBv2.DocumentModel.Document.FromAttributeMap(response.Items.FirstOrDefault());

            return JsonConvert.DeserializeObject<InvestmentCustomer>(document.ToJson());
        }

        internal static async Task CorrecaoDadosCustomer(AmazonDynamoDBClient _dynamoDbClient)
        {
            Console.WriteLine($"Estamos atualizando os dados...");
            Console.WriteLine($"Isso pode levar alguns minutos");

            List<InvestmentCustomer> investmentCustomers = await GetInvestmentCustomerEmailEmpty(_dynamoDbClient);

            List<DadosAtualizadosResponse> results = new List<DadosAtualizadosResponse>();

            foreach (InvestmentCustomer investmentCustomer in investmentCustomers)
            {
                Customer customer = await GetCustomerCustomer(_dynamoDbClient, investmentCustomer.CustomerId);

                if (customer != null)
                {
                    investmentCustomer.Email = customer.MainEmailAddress;
                    investmentCustomer.Name = customer.Name;

                    await UpdateInvestmentCustomer(_dynamoDbClient, investmentCustomer);
                    results.Add(new DadosAtualizadosResponse { CustomerId = investmentCustomer.CustomerId, Type = "Email" });
                }
            }


            Console.WriteLine($"Email dos clientes atualizados...");

            Console.WriteLine($"Verificando mais dados obsoletos...");

            Console.WriteLine($"Isso pode levar alguns minutos");

            List<InvestmentCustomer> investmentCustomersNames = await GetInvestmentCustomerNameEmpty(_dynamoDbClient);

            foreach (InvestmentCustomer investmentCustomer in investmentCustomersNames)
            {
                Customer customer = await GetCustomerCustomer(_dynamoDbClient, investmentCustomer.CustomerId);

                if (customer != null)
                {
                    investmentCustomer.Name = customer.Name;

                    await UpdateInvestmentCustomer(_dynamoDbClient, investmentCustomer);
                    results.Add(new DadosAtualizadosResponse { CustomerId = investmentCustomer.CustomerId, Type = "Name" });
                }
            }

            Console.WriteLine($"Nomes dos clientes atualizados...");

            Console.WriteLine($"Verificando mais dados obsoletos...");

            Console.WriteLine($"Isso pode levar alguns minutos");

            List<InvestmentCustomer> investmentCustomersAccount = await GetInvestmentCustomerAccountEmpty(_dynamoDbClient);

            foreach (InvestmentCustomer investmentCustomer in investmentCustomersAccount)
            {
                CheckingAccount customer = await GetCheckingAccount(_dynamoDbClient, investmentCustomer.CustomerId);

                if (customer != null)
                {
                    investmentCustomer.Account = customer.Id;

                    await UpdateInvestmentCustomer(_dynamoDbClient, investmentCustomer);
                    results.Add(new DadosAtualizadosResponse { CustomerId = investmentCustomer.CustomerId, Type = "Account" });
                }
            }

            Console.WriteLine($"Accounts dos clientes atualizados...");

            Console.WriteLine($"Todos os dados atualizados...");

            Console.WriteLine($"Gerando arquivo de conprovacao...");

            GenerateFileClientesAtualizados(results, $"ListaClientesAtualizados_{DateTime.Now.ToString("dd-MM-yyyy_HH-mm")}");

            Console.WriteLine($"Um arquivo foi criado com a lista dos Ids atualizados");
        }

        internal static async Task<List<InvestmentCustomer>> GetInvestmentCustomerEmailEmpty(AmazonDynamoDBClient _dynamoDbClient)
        {
            Dictionary<string, AttributeValue> lastKeyEvaluated = null;

            List<InvestmentCustomer> events = new List<InvestmentCustomer>();

            ScanFilter filter = new ScanFilter();

            List<AttributeValue> att = new List<AttributeValue>
            {
                new AttributeValue() {NULL = true }
            };

            //filter.AddCondition("Email", ScanOperator.Equal, att);
            filter.AddCondition("Email", ScanOperator.Equal, att);

            do
            {
                ScanRequest request = new ScanRequest
                {
                    TableName = "Barigui.Services.Investment_Customer",
                    ScanFilter = filter.ToConditions(),
                    Limit = 100,
                    ExclusiveStartKey = lastKeyEvaluated,
                };

                ScanResponse response = await _dynamoDbClient.ScanAsync(request);

                foreach (Dictionary<string, AttributeValue> item in response.Items)
                {
                    Document document = Amazon.DynamoDBv2.DocumentModel.Document.FromAttributeMap(item);
                    events.Add(JsonConvert.DeserializeObject<InvestmentCustomer>(document.ToJson()));
                }

                lastKeyEvaluated = response.LastEvaluatedKey;

            } while (lastKeyEvaluated != null && lastKeyEvaluated.Count != 0);

            return events;
        }

        internal static async Task<List<InvestmentCustomer>> GetInvestmentCustomerNameEmpty(AmazonDynamoDBClient _dynamoDbClient)
        {
            Dictionary<string, AttributeValue> lastKeyEvaluated = null;

            List<InvestmentCustomer> events = new List<InvestmentCustomer>();

            ScanFilter filter = new ScanFilter();

            List<AttributeValue> att = new List<AttributeValue>
            {
                new AttributeValue() {NULL = true }
            };

            //filter.AddCondition("Email", ScanOperator.Equal, att);
            filter.AddCondition("Name", ScanOperator.Equal, att);

            do
            {
                ScanRequest request = new ScanRequest
                {
                    TableName = "Barigui.Services.Investment_Customer",
                    ScanFilter = filter.ToConditions(),
                    Limit = 100,
                    ExclusiveStartKey = lastKeyEvaluated,
                };

                ScanResponse response = await _dynamoDbClient.ScanAsync(request);

                foreach (Dictionary<string, AttributeValue> item in response.Items)
                {
                    Document document = Amazon.DynamoDBv2.DocumentModel.Document.FromAttributeMap(item);
                    events.Add(JsonConvert.DeserializeObject<InvestmentCustomer>(document.ToJson()));
                }

                lastKeyEvaluated = response.LastEvaluatedKey;

            } while (lastKeyEvaluated != null && lastKeyEvaluated.Count != 0);

            return events;
        }

        internal static async Task<List<InvestmentCustomer>> GetInvestmentCustomerAccountEmpty(AmazonDynamoDBClient _dynamoDbClient)
        {
            Dictionary<string, AttributeValue> lastKeyEvaluated = null;

            List<InvestmentCustomer> events = new List<InvestmentCustomer>();

            ScanFilter filter = new ScanFilter();

            filter.AddCondition("Account", ScanOperator.Equal, 0);

            do
            {
                ScanRequest request = new ScanRequest
                {
                    TableName = "Barigui.Services.Investment_Customer",
                    ScanFilter = filter.ToConditions(),
                    Limit = 100,
                    ExclusiveStartKey = lastKeyEvaluated,
                };

                ScanResponse response = await _dynamoDbClient.ScanAsync(request);

                foreach (Dictionary<string, AttributeValue> item in response.Items)
                {
                    Document document = Amazon.DynamoDBv2.DocumentModel.Document.FromAttributeMap(item);
                    events.Add(JsonConvert.DeserializeObject<InvestmentCustomer>(document.ToJson()));
                }

                lastKeyEvaluated = response.LastEvaluatedKey;

            } while (lastKeyEvaluated != null && lastKeyEvaluated.Count != 0);

            return events;
        }

        internal static async Task<Customer> GetCustomerCustomer(AmazonDynamoDBClient _dynamoDbClient, string customerId)
        {
            Dictionary<string, AttributeValue> lastKeyEvaluated = null;

            List<Customer> events = new List<Customer>();

            ScanFilter filter = new ScanFilter();

            filter.AddCondition("Id", ScanOperator.Equal, customerId);

            do
            {
                ScanRequest request = new ScanRequest
                {
                    TableName = "Barigui.Services.Customer_Customers",
                    ScanFilter = filter.ToConditions(),
                    Limit = 100,
                    ExclusiveStartKey = lastKeyEvaluated,
                };

                ScanResponse response = await _dynamoDbClient.ScanAsync(request);

                if (response.Items.Any())
                {
                    Document document = Amazon.DynamoDBv2.DocumentModel.Document.FromAttributeMap(response.Items.FirstOrDefault());
                    return JsonConvert.DeserializeObject<Customer>(document.ToJson());
                }

                lastKeyEvaluated = response.LastEvaluatedKey;

            } while (lastKeyEvaluated != null && lastKeyEvaluated.Count != 0);

            return null;
        }

        internal static async Task<CheckingAccount> GetCheckingAccount(AmazonDynamoDBClient _dynamoDbClient, string customerId)
        {
            Dictionary<string, AttributeValue> lastKeyEvaluated = null;

            List<CheckingAccount> events = new List<CheckingAccount>();

            ScanFilter filter = new ScanFilter();

            filter.AddCondition("CustomerId", ScanOperator.Equal, customerId);

            do
            {
                ScanRequest request = new ScanRequest
                {
                    TableName = "Barigui.Services.Account_CheckingAccount",
                    ScanFilter = filter.ToConditions(),
                    Limit = 100,
                    ExclusiveStartKey = lastKeyEvaluated,
                };

                ScanResponse response = await _dynamoDbClient.ScanAsync(request);

                if (response.Items.Any())
                {
                    Document document = Amazon.DynamoDBv2.DocumentModel.Document.FromAttributeMap(response.Items.FirstOrDefault());
                    return JsonConvert.DeserializeObject<CheckingAccount>(document.ToJson());
                }

                lastKeyEvaluated = response.LastEvaluatedKey;

            } while (lastKeyEvaluated != null && lastKeyEvaluated.Count != 0);

            return null;
        }

        internal static async Task UpdateInvestmentCustomer(AmazonDynamoDBClient _dynamoDbClient, InvestmentCustomer customer)
        {
            Document item = Document.FromJson(JsonConvert.SerializeObject(customer));

            PutItemRequest request = new PutItemRequest
            {
                TableName = "Barigui.Services.Investment_Customer",
                Item = item.ToAttributeMap()
            };

            await _dynamoDbClient.PutItemAsync(request);
        }

        internal static async Task GetLogsAplicacaoUser(AmazonDynamoDBClient _dynamoDbClient)
        {
            Console.WriteLine($"digite a key para a messageId");
            string key = Console.ReadLine();

            Console.WriteLine($"Buscando dados...");
            Console.WriteLine($"Isso pode levar alguns minutos");

            List<EventStore> eventlogs = await GetEventLogs(_dynamoDbClient, key);

            List<AssetPurchased> results = new List<AssetPurchased>();

            object lockMe = new object();
            ParallelOptions opts = new ParallelOptions { MaxDegreeOfParallelism = Convert.ToInt32(Math.Ceiling((Environment.ProcessorCount * 0.75) * 1.0)) };


            Parallel.ForEach(eventlogs, opts, log =>
                {
                    lock (lockMe)
                    {
                        byte[] data = Convert.FromBase64String(log.Payload);
                        string decodedString = Encoding.UTF8.GetString(data);

                        string teste = Regex.Replace(decodedString, @"[^\u0000-\u007F]+", string.Empty);

                        if (decodedString.Contains(".AssetPurchased"))
                        {
                            string tituloregex = Regex.Replace(decodedString.Substring(155, 7), @"[^\d]", "");
                            //var titulo = tituloregex.Length <= 6 ? tituloregex : tituloregex.Substring(0, 6);

                            results.Add(new AssetPurchased
                            {
                                Cliente = decodedString.Substring(2, 11),
                                OperationId = decodedString.Substring(118, 36).Replace("\"", ""),
                                TipoOperacao = "Aplicação",
                                Titulo = tituloregex,
                                DataOperação = DateTime.SpecifyKind(DateTime.Parse(log.MessageId.Substring(12, log.MessageId.Length - 12)), DateTimeKind.Local).ToString()
                            });
                        }
                    }
                }
            );

            Console.WriteLine($"Dados gerados");
            Console.WriteLine($"Gerando arquivo...");

            GenerateFileLogApply(results, $"{key}_aplicacaoLogs");

        }

        internal static async Task GetProfileSpecificUser(AmazonDynamoDBClient _dynamoDbClient)
        {
            Console.WriteLine($"digite os cpfs divididos por virgula e sem espaco (99999999999,88888888888,....)");
            string cpfs = Console.ReadLine();

            Console.WriteLine($"Gerando dados estatisticos...");

            List<CustomerSuitabilityHistory> AllLeads = await GetSuitabilityHistory(_dynamoDbClient);
            string[] cpfList = cpfs.Replace(" ", "").Split(",");

            List<CustomerSuitabilityHistory> leadsList = new List<CustomerSuitabilityHistory>();

            foreach (string item in cpfList)
            {
                CustomerSuitabilityHistory databaseLead = AllLeads.FirstOrDefault(x => x.CustomerId.Equals(item));
                if (databaseLead != null)
                {
                    leadsList.Add(databaseLead);
                }
            }

            Verify(leadsList, "2");

            Console.WriteLine($"Será criado um arquivos no diretorio");
        }

        internal static async Task GetFullProfileSpecificUser(AmazonDynamoDBClient _dynamoDbClient)
        {
            Console.WriteLine($"digite os cpfs divididos por virgula e sem espaco (99999999999,88888888888,....)");
            string cpfs = Console.ReadLine();
            List<CustomerSuitabilityHistory> allCustomerSuitabilityHistory = await GetSuitabilityHistory(_dynamoDbClient);
            string[] cpfList = cpfs.Replace(" ", "").Split(",");

            List<CustomerSuitabilityHistory> leadsList = new List<CustomerSuitabilityHistory>();

            foreach (string item in cpfList)
            {
                CustomerSuitabilityHistory databaseLead = allCustomerSuitabilityHistory.FirstOrDefault(x => x.CustomerId.Equals(item));
                if (databaseLead != null)
                {
                    leadsList.Add(databaseLead);
                }
            }

            Console.WriteLine($"Gerando dados estatisticos...");

            await VerifySuitabilityHistory(leadsList, "0", _dynamoDbClient);

            Console.WriteLine($"Será criado {leadsList.Count()} arquivos no diretorio");
        }

        internal static async Task<List<DatabaseSuitability>> GetCustomerSuitabilities(AmazonDynamoDBClient _dynamoDbClient)
        {
            Dictionary<string, AttributeValue> lastKeyEvaluated = null;

            List<DatabaseSuitability> leads = new List<DatabaseSuitability>();

            do
            {
                ScanRequest request = new ScanRequest
                {
                    TableName = "Barigui.Services.Investment_CustomerSuitability",
                    Limit = 100,
                    ExclusiveStartKey = lastKeyEvaluated,
                };

                ScanResponse response = await _dynamoDbClient.ScanAsync(request);

                foreach (Dictionary<string, AttributeValue> item in response.Items)
                {
                    Document document = Amazon.DynamoDBv2.DocumentModel.Document.FromAttributeMap(item);
                    leads.Add(JsonConvert.DeserializeObject<DatabaseSuitability>(document.ToJson()));
                }

                lastKeyEvaluated = response.LastEvaluatedKey;

            } while (lastKeyEvaluated != null && lastKeyEvaluated.Count != 0);

            return leads;
        }

        internal static async Task<Suitability> GetSuitability(AmazonDynamoDBClient _dynamoDbClient, string id)
        {

            GetItemRequest request = new GetItemRequest
            {
                TableName = "Barigui.Services.Investment_Suitability",
                Key = new Dictionary<string, AttributeValue>
                {
                    { "Id", new AttributeValue { S = id } }
                }
            };

            GetItemResponse search = await _dynamoDbClient.GetItemAsync(request);

            if (!search.Item.Any())
            {
                return null;
            }

            Document document = Document.FromAttributeMap(search.Item);

            Suitability suitability = JsonConvert.DeserializeObject<Suitability>(document?.ToJson());

            return suitability;
        }

        internal static async Task<List<CustomerSuitabilityHistory>> GetSuitabilityHistory(AmazonDynamoDBClient _dynamoDbClient)
        {
            Dictionary<string, AttributeValue> lastKeyEvaluated = null;

            List<CustomerSuitabilityHistory> leads = new List<CustomerSuitabilityHistory>();

            do
            {
                ScanRequest request = new ScanRequest
                {
                    TableName = "Barigui.Services.Investment_CustomerSuitabilityHistory",
                    Limit = 100,
                    ExclusiveStartKey = lastKeyEvaluated,
                };

                ScanResponse response = await _dynamoDbClient.ScanAsync(request);

                ScanRequest requestTerm = new ScanRequest
                {
                    TableName = "Barigui.Services.Customer_Terms",
                    Limit = 100,
                    ExclusiveStartKey = lastKeyEvaluated,
                };

                ScanResponse responseTerm = await _dynamoDbClient.ScanAsync(requestTerm);

                List<CostumerTerms> terms = new List<CostumerTerms>();

                terms = responseTerm.Items
                    .Select(Amazon.DynamoDBv2.DocumentModel.Document.FromAttributeMap)
                    .Select(d => d.ToJson())
                    .Select(JsonConvert.DeserializeObject<CostumerTerms>).ToList();

                foreach (Dictionary<string, AttributeValue> item in response.Items)
                {
                    Document document = Amazon.DynamoDBv2.DocumentModel.Document.FromAttributeMap(item);
                    CustomerSuitabilityHistory history = JsonConvert.DeserializeObject<CustomerSuitabilityHistory>(document.ToJson());

                    foreach (CostumerTerms term in terms)
                    {
                        if (term.Id == history.CustomerId)
                        {
                            TermsAcceptance firstUserTerm = term.TermsAcceptance.Where(c => c.Type == "INVESTOR_QUALIFIED").FirstOrDefault();
                            List<TermsAcceptance> listUserTerm = term.TermsAcceptance.Where(c => c.Type == "INVESTOR_QUALIFIED").ToList();
                            history.DateTermInvestidorQualified = firstUserTerm != null ? firstUserTerm.AcceptanceDate.ToString() : string.Empty;
                            history.FisrtTimeAnsweredTerm = listUserTerm.Any() ? true : false;
                        }
                    }
                    leads.Add(history);
                }

                lastKeyEvaluated = response.LastEvaluatedKey;

            } while (lastKeyEvaluated != null && lastKeyEvaluated.Count != 0);

            return leads;
        }

        internal static async Task<List<EventStore>> GetEventLogs(AmazonDynamoDBClient _dynamoDbClient, string messageId)
        {
            Dictionary<string, AttributeValue> lastKeyEvaluated = null;

            List<EventStore> events = new List<EventStore>();

            ScanFilter filter = new ScanFilter();

            filter.AddCondition("Topic", ScanOperator.BeginsWith, "Barigui_Services_Investment");
            filter.AddCondition("MessageId", ScanOperator.BeginsWith, messageId);

            do
            {
                ScanRequest request = new ScanRequest
                {
                    TableName = "Bari.EventStore",
                    ScanFilter = filter.ToConditions(),
                    Limit = 100,
                    ExclusiveStartKey = lastKeyEvaluated,
                };

                ScanResponse response = await _dynamoDbClient.ScanAsync(request);

                foreach (Dictionary<string, AttributeValue> item in response.Items)
                {
                    Document document = Amazon.DynamoDBv2.DocumentModel.Document.FromAttributeMap(item);
                    events.Add(JsonConvert.DeserializeObject<EventStore>(document.ToJson()));
                }

                lastKeyEvaluated = response.LastEvaluatedKey;

            } while (lastKeyEvaluated != null && lastKeyEvaluated.Count != 0);

            return events;
        }

        internal static void Verify(List<CustomerSuitabilityHistory> leads, string opt)
        {
            List<DatabaseSuitability> listaSuitability = new List<DatabaseSuitability>();


            foreach (CustomerSuitabilityHistory lead in leads)
            {
                IOrderedEnumerable<CustomerSuitabilityHistoryAnswers> suitabilityOrdered = lead.CustomerSuitabilities.OrderBy(x => x.AnsweredAt);
                CustomerSuitabilityHistoryAnswers firstSuitability = suitabilityOrdered.FirstOrDefault();
                CustomerSuitabilityHistoryAnswers LastSuitability = suitabilityOrdered.LastOrDefault();
                listaSuitability.Add(new DatabaseSuitability
                {
                    ChangedDate = firstSuitability.AnsweredAt,
                    CustomerId = lead.CustomerId,
                    CustomerProfile = ProfileResolver(firstSuitability.Profile),
                    FistTimeAnsweredSuitability = firstSuitability.AnsweredAt == LastSuitability.AnsweredAt ? "Sim" : "Nao",
                    LastCustomerProfile = ProfileResolver(LastSuitability.Profile),
                    TermInfestorQualified = lead.DateTermInvestidorQualified,
                    FirstTimeAnsweredTerm = string.IsNullOrEmpty(lead.DateTermInvestidorQualified) ? string.Empty : lead.FisrtTimeAnsweredTerm ? "Sim" : "Nao"
                }); ;
            }

            if (opt.Equals("1"))
            {
                GenerateFile(listaSuitability, "clientes_suitability_listagem_completa");
            }
            else
            {
                GenerateFile(listaSuitability, "clientes_suitability_listagem_por_cpfs");
            }
        }

        internal static async Task VerifySuitabilityHistory(List<CustomerSuitabilityHistory> suitabilities, string opt, AmazonDynamoDBClient _dynamoDbClient)
        {
            foreach (CustomerSuitabilityHistory suitabilityHistory in suitabilities)
            {
                List<ReportQuestion> listaSuitability = new List<ReportQuestion>();
                CustomerSuitabilityHistoryAnswers firstSuitability = suitabilityHistory.CustomerSuitabilities.OrderBy(x => x.AnsweredAt).FirstOrDefault();

                if (firstSuitability is null)
                {
                    continue;
                }

                Suitability suitability = await GetSuitability(_dynamoDbClient, firstSuitability.SuitabilityId.ToString().ToUpper());

                if (suitability is null)
                {
                    continue;
                }

                foreach (CustomerSuitabilityQuestionAnswer answer in firstSuitability.Answers)
                {
                    SuitabilityQuestions question = suitability.Questions.FirstOrDefault(x => x.Id.Equals(answer.QuestionId));

                    if (question is null)
                    {
                        continue;
                    }

                    if (answer.SubquestionsAnswer.Any())
                    {
                        listaSuitability.Add(new ReportQuestion
                        {
                            Customer = suitabilityHistory.CustomerId,
                            Question = question.Content,
                            answer = " ",
                            Profile = ProfileResolver(firstSuitability.Profile),
                            SuitabilityDate = DateTime.SpecifyKind(firstSuitability.AnsweredAt, DateTimeKind.Local)
                        });

                        foreach (CustomerSuitabilityQuestionAnswer subquestion in answer.SubquestionsAnswer)
                        {
                            SuitabilityQuestions subquestionFiltered = question.Options.FirstOrDefault(x => x.SubQuestions.Any()).SubQuestions.FirstOrDefault(x => x.Id.Equals(subquestion.QuestionId));
                            string questionContent = subquestionFiltered.Content;
                            SuitabilityQuestionsOptions respostaContent = subquestionFiltered.Options.FirstOrDefault(x => x.Id.Equals(subquestion.AnswerId));

                            listaSuitability.Add(new ReportQuestion
                            {
                                Customer = suitabilityHistory.CustomerId,
                                Question = questionContent,
                                answer = respostaContent.Content,
                                Profile = ProfileResolver(firstSuitability.Profile),
                                SuitabilityDate = DateTime.SpecifyKind(firstSuitability.AnsweredAt, DateTimeKind.Local),
                                SuitabilityScore = firstSuitability.Score.ToString(),
                                Optionscore = respostaContent.Score.ToString()

                            });
                        }
                    }
                    else
                    {
                        SuitabilityQuestionsOptions resposta = question.Options.FirstOrDefault(x => x.Id.Equals(answer.AnswerId));

                        if (resposta is null)
                        {
                            continue;
                        }

                        listaSuitability.Add(new ReportQuestion
                        {
                            Customer = suitabilityHistory.CustomerId,
                            Question = question.Content,
                            answer = resposta.Content,
                            Profile = ProfileResolver(firstSuitability.Profile),
                            SuitabilityDate = DateTime.SpecifyKind(firstSuitability.AnsweredAt, DateTimeKind.Local),
                            SuitabilityScore = firstSuitability.Score.ToString(),
                            Optionscore = resposta.Score.ToString()
                        });
                    }
                }

                GenerateFileSuitabilityWithAnswer(listaSuitability, $"{suitabilityHistory.CustomerId}_Suitability_com_respostas");
            }
        }

        internal static void GenerateFile(List<DatabaseSuitability> data, string fileName)
        {
            string[] _headers = { "Cliente", "Data do primeiro suitability realizado", "Perfil", "Primeira vez que respondeu suitability", "Perfil ultimo suitability", "Date do preenchimento Termo de investidor Qualificado", "Primeira vez respondeu o Termo" };

            using (MemoryStream memoryStream = new MemoryStream())
            using (StreamWriter streamWriter = new StreamWriter(memoryStream))
            using (CsvWriter csvWriter = new CsvWriter(streamWriter))
            {
                csvWriter.WriteField(_headers);
                csvWriter.NextRecord();

                foreach (DatabaseSuitability item in data)
                {
                    csvWriter.WriteField(item.CustomerId);
                    csvWriter.WriteField(item.ChangedDate);
                    csvWriter.WriteField(item.CustomerProfile);
                    csvWriter.WriteField(item.FistTimeAnsweredSuitability);
                    csvWriter.WriteField(item.LastCustomerProfile);
                    csvWriter.WriteField(item.TermInfestorQualified);
                    csvWriter.WriteField(item.FirstTimeAnsweredTerm);
                    csvWriter.NextRecord();

                }

                streamWriter.Flush();
                memoryStream.Position = 0;

                FileStream fileStream = new FileStream($"{fileName}.csv", FileMode.Create, FileAccess.Write);
                memoryStream.CopyTo(fileStream);
                fileStream.Dispose();
            }
        }

        internal static void GenerateFile(List<MovimentoUsuario> data, string fileName)
        {
            Console.WriteLine("Gerando o arquivo.csv");
            string[] _headers = { "Cliente", "Tem Movimentacao", "Primeira Aplicacao", "Titulo Ativo" };

            using (MemoryStream memoryStream = new MemoryStream())
            using (StreamWriter streamWriter = new StreamWriter(memoryStream))
            using (CsvWriter csvWriter = new CsvWriter(streamWriter))
            {
                csvWriter.WriteField(_headers);
                csvWriter.NextRecord();

                foreach (MovimentoUsuario item in data)
                {
                    csvWriter.WriteField(item.CPF_CNPJ);
                    csvWriter.WriteField(item.MOVIMENTACOES);
                    csvWriter.WriteField(item.PRIMEIRA_APLICACAO);
                    csvWriter.WriteField(item.TITULOS_ATIVOS);
                    csvWriter.NextRecord();
                }

                streamWriter.Flush();
                memoryStream.Position = 0;

                FileStream fileStream = new FileStream($"{fileName}.csv", FileMode.Create, FileAccess.Write);
                memoryStream.CopyTo(fileStream);
                fileStream.Dispose();
                Console.WriteLine($"Arquivo gerado com sucesso na pasta {fileStream.Name}");
            }
        }

        internal static void GenerateFileSuitabilityWithAnswer(List<ReportQuestion> data, string fileName)
        {
            string[] _headers = { "Cliente", "Data do suitability realizado", "Perfil", "Pergunta", "Resposta", "Score da resposta", "Score GERAL do cliente" };

            using (MemoryStream memoryStream = new MemoryStream())
            using (StreamWriter streamWriter = new StreamWriter(memoryStream, System.Text.Encoding.UTF8))
            using (CsvWriter csvWriter = new CsvWriter(streamWriter))
            {
                csvWriter.WriteField(_headers);
                csvWriter.NextRecord();

                foreach (ReportQuestion item in data)
                {
                    csvWriter.WriteField(item.Customer);
                    csvWriter.WriteField(item.SuitabilityDate);
                    csvWriter.WriteField(item.Profile);
                    csvWriter.WriteField(item.Question);
                    csvWriter.WriteField(item.answer);
                    csvWriter.WriteField(item.Optionscore);
                    csvWriter.WriteField(item.SuitabilityScore);
                    csvWriter.NextRecord();
                }

                streamWriter.Flush();
                memoryStream.Position = 0;

                FileStream fileStream = new FileStream($"{fileName}.csv", FileMode.Create, FileAccess.Write);
                memoryStream.CopyTo(fileStream);
                fileStream.Dispose();
            }
        }

        internal static void GenerateFileLogApply(List<AssetPurchased> data, string fileName)
        {
            string[] _headers = { "Id da operação", "Cliente", "Data da operação", "Tipo de operação", "Título" };

            using (MemoryStream memoryStream = new MemoryStream())
            using (StreamWriter streamWriter = new StreamWriter(memoryStream, System.Text.Encoding.UTF8))
            using (CsvWriter csvWriter = new CsvWriter(streamWriter))
            {
                csvWriter.WriteField(_headers);
                csvWriter.NextRecord();

                foreach (AssetPurchased item in data)
                {
                    csvWriter.WriteField(item.OperationId);
                    csvWriter.WriteField(item.Cliente);
                    csvWriter.WriteField(item.DataOperação);
                    csvWriter.WriteField(item.TipoOperacao);
                    csvWriter.WriteField(item.Titulo);
                    csvWriter.NextRecord();
                }

                streamWriter.Flush();
                memoryStream.Position = 0;

                FileStream fileStream = new FileStream($"{fileName}.csv", FileMode.Create, FileAccess.Write);
                memoryStream.CopyTo(fileStream);
                fileStream.Dispose();
            }

            Console.WriteLine($"Arquivo gerado com o nome => {fileName}");
        }

        internal static void GenerateFileClientesAtualizados(List<DadosAtualizadosResponse> data, string fileName)
        {
            string[] _headers = { "Cliente", "Tipo de atualizacao" };

            using (MemoryStream memoryStream = new MemoryStream())
            using (StreamWriter streamWriter = new StreamWriter(memoryStream, System.Text.Encoding.UTF8))
            using (CsvWriter csvWriter = new CsvWriter(streamWriter))
            {
                csvWriter.WriteField(_headers);
                csvWriter.NextRecord();

                foreach (DadosAtualizadosResponse item in data)
                {
                    csvWriter.WriteField(item.CustomerId);
                    csvWriter.WriteField(item.Type);
                    csvWriter.NextRecord();
                }

                streamWriter.Flush();
                memoryStream.Position = 0;

                FileStream fileStream = new FileStream($"{fileName}.csv", FileMode.Create, FileAccess.Write);
                memoryStream.CopyTo(fileStream);
                fileStream.Dispose();
            }

            Console.WriteLine($"Arquivo gerado com o nome => {fileName}");
        }

        internal static string ProfileResolver(string profile)
        {
            if (profile.ToUpper().Equals("Conservative".ToUpper()))
            {
                return "Conservador";
            }

            if (profile.ToUpper().Equals("Moderate".ToUpper()))
            {
                return "Moderado";
            }
            if (profile.ToUpper().Equals("Aggressive".ToUpper()))
            {
                return "Arrojado";
            }

            return "ERRO";
        }

        internal static async Task GetFullProfileaLLUserS(AmazonDynamoDBClient _dynamoDbClient)
        {
            Console.WriteLine($"Buscando clientes com suitability..... isso pode levar alguns minutos");
            List<CustomerSuitabilityHistory> allCustomerSuitabilityHistory = await GetSuitabilityHistory(_dynamoDbClient);

            List<CustomerSuitabilityHistory> leadsList = new List<CustomerSuitabilityHistory>();

            Console.WriteLine($"Gerando dados estatisticos...");

            await VerifyAllSuitabilityHistory(allCustomerSuitabilityHistory, "0", _dynamoDbClient);
        }

        internal static async Task VerifyAllSuitabilityHistory(List<CustomerSuitabilityHistory> suitabilities, string opt, AmazonDynamoDBClient _dynamoDbClient)
        {
            List<ReportQuestion> listToCriateFile = new List<ReportQuestion>();
            foreach (CustomerSuitabilityHistory suitabilityHistory in suitabilities)
            {
                List<ReportQuestion> listaSuitability = new List<ReportQuestion>();
                CustomerSuitabilityHistoryAnswers lastSuitability = suitabilityHistory.CustomerSuitabilities.OrderByDescending(x => x.AnsweredAt).FirstOrDefault();

                if (lastSuitability is null)
                {
                    continue;
                }

                Suitability suitability = await GetSuitability(_dynamoDbClient, lastSuitability.SuitabilityId.ToString().ToUpper());

                if (suitability is null)
                {
                    continue;
                }

                foreach (CustomerSuitabilityQuestionAnswer answer in lastSuitability.Answers)
                {
                    SuitabilityQuestions question = suitability.Questions.FirstOrDefault(x => x.Id.Equals(answer.QuestionId));

                    if (question is null)
                    {
                        continue;
                    }

                    if (answer.SubquestionsAnswer.Any())
                    {
                        listaSuitability.Add(new ReportQuestion
                        {
                            Customer = suitabilityHistory.CustomerId,
                            Question = question.Content,
                            answer = " ",
                            Profile = ProfileResolver(lastSuitability.Profile),
                            SuitabilityDate = DateTime.SpecifyKind(lastSuitability.AnsweredAt, DateTimeKind.Local)
                        });

                        foreach (CustomerSuitabilityQuestionAnswer subquestion in answer.SubquestionsAnswer)
                        {
                            SuitabilityQuestions subquestionFiltered = question.Options.FirstOrDefault(x => x.SubQuestions.Any()).SubQuestions.FirstOrDefault(x => x.Id.Equals(subquestion.QuestionId));
                            string questionContent = subquestionFiltered.Content;
                            SuitabilityQuestionsOptions respostaContent = subquestionFiltered.Options.FirstOrDefault(x => x.Id.Equals(subquestion.AnswerId));

                            listaSuitability.Add(new ReportQuestion
                            {
                                Customer = suitabilityHistory.CustomerId,
                                Question = questionContent,
                                answer = respostaContent.Content,
                                Profile = ProfileResolver(lastSuitability.Profile),
                                SuitabilityDate = DateTime.SpecifyKind(lastSuitability.AnsweredAt, DateTimeKind.Local),
                                SuitabilityScore = lastSuitability.Score.ToString(),
                                Optionscore = respostaContent.Score.ToString()
                            });
                        }
                    }
                    else
                    {
                        SuitabilityQuestionsOptions resposta = question.Options.FirstOrDefault(x => x.Id.Equals(answer.AnswerId));

                        if (resposta is null)
                        {
                            continue;
                        }

                        listaSuitability.Add(new ReportQuestion
                        {
                            Customer = suitabilityHistory.CustomerId,
                            Question = question.Content,
                            answer = resposta.Content,
                            Profile = ProfileResolver(lastSuitability.Profile),
                            SuitabilityDate = DateTime.SpecifyKind(lastSuitability.AnsweredAt, DateTimeKind.Local),
                            SuitabilityScore = lastSuitability.Score.ToString(),
                            Optionscore = resposta.Score.ToString()

                        });
                    }
                }

                listToCriateFile.AddRange(listaSuitability);
            }

            int ItemsPerPage = 1000000;
            int TotalItems = listToCriateFile.Count();
            int TotalPages = (int)Math.Ceiling((double)TotalItems / ItemsPerPage);

            Console.WriteLine($"Será criado {TotalPages} arquivos no diretorio com até {ItemsPerPage} linhas");

            for (int page = 0; page < TotalPages; page++)
            {
                IEnumerable<ReportQuestion> item = listToCriateFile.Skip(ItemsPerPage * page).Take(ItemsPerPage);
                GenerateFileSuitabilityWithAnswer(listToCriateFile, $"Suitability_com_respostas_clientes_Page_{(page + 1)}");
            }
        }
    }
}