using System;
using System.Collections.Generic;
using System.Text;

namespace OnboardingDashboard
{
    public class InvestmentCustomer
    {
        public string CustomerId { get; set; }
        public long Account { get; set; }
        public int Agency { get; set; }
        public string Name { get; set; }
        public string Email { get; set; }
        public bool SendWithdrawInvoice { get; set; }
    }
}