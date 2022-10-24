using System;
using System.Collections.Generic;
using System.Text;

namespace OnboardingDashboard
{
    public class SuitabilityQuestions
    {
        public string Id { get; set; }
        public string Content { get; set; }
        public IEnumerable<SuitabilityQuestionsOptions> Options { get; set; }
    }
}
