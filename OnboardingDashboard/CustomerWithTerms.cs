using System;
using System.Collections.Generic;
using System.Text;

namespace OnboardingDashboard
{
    public class CustomerWithTerms
    {
        public string CustomerId { get; set; }
        public string Profile { get; set; }
        public DateTime AnsweredAt { get; set; }
        public bool IsFirstSuitability{ get; set; }
        public DateTime CriTermAssign { get; set; }
        public bool IsFirstTimeTerm { get; set; }
    }
}
