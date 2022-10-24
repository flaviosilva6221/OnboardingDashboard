using System.Collections.Generic;

namespace OnboardingDashboard
{
    public class CustomerSuitabilityQuestionAnswer
    {
        public string QuestionId { get; set; }
        public string AnswerId { get; set; }
        public IEnumerable<CustomerSuitabilityQuestionAnswer> SubquestionsAnswer { get; set; }
    }
}
