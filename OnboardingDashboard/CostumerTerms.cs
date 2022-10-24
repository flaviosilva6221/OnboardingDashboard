using System;
using System.Collections.Generic;

namespace OnboardingDashboard
{
    public class CostumerTerms
    {
        public string Id { get; set; }
        public List<TermsAcceptance> TermsAcceptance { get; set; }
    }

    public class TermsAcceptance
    {
        public DateTime AcceptanceDate { get; set; }
        public string Contract { get; set; }
        public string Type { get; set; }
    }
}
