using System.Collections.Generic;

namespace OnboardingDashboard
{
    public class CustomerSuitabilityHistory
    {
        public string CustomerId { get; set; }
        public IEnumerable<CustomerSuitabilityHistoryAnswers> CustomerSuitabilities { get; set; }
        public string DateTermInvestidorQualified { get; set; }
        public bool FisrtTimeAnsweredTerm { get; set; }
    }
}
