using System;
using System.Collections.Generic;
using System.Text;

namespace OnboardingDashboard
{
    public class Suitability
    {
        public string Id { get; set; }
        public bool IsActive { get; set; }
        public DateTime CreatedDate { get; set; }
        public IEnumerable<SuitabilityQuestions> Questions { get; set; }
    }
}
