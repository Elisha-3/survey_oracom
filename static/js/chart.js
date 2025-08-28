document.addEventListener("DOMContentLoaded", function() {
  loadCharts();

  // Handling upload form submission
  document.getElementById('uploadForm').addEventListener('submit', function(e) {
    e.preventDefault();
    const formData = new FormData(this);
    const responseMessage = document.getElementById('responseMessage');
    responseMessage.textContent = 'Uploading...';
    responseMessage.className = 'mt-3 text-info';

    fetch('/upload', {
      method: 'POST',
      body: formData,
      contentType: false,
      processData: false
    })
    .then(response => response.json())
    .then(data => {
      responseMessage.textContent = data.message;
      responseMessage.className = 'mt-3 text-success';
      loadCharts();
    })
    .catch(error => {
      responseMessage.textContent = error.message || 'Upload failed';
      responseMessage.className = 'mt-3 text-danger';
    });
  });

  function loadCharts() {
    fetch('/api/data')
      .then(response => response.json())
      .then(data => {
        const donutCtx = document.getElementById('donutChart').getContext('2d');
        new Chart(donutCtx, {
          type: 'pie',
          data: {
            labels: ['Q1', 'Q2', 'Q3'],
            datasets: [{
              label: 'Q1 Distribution',
              data: [data.q1_dist.Q1 * 100, data.q1_dist.Q2 * 100, data.q1_dist.Q3 * 100],
              backgroundColor: ['#36A2EB', '#FF6384', '#FFCE56']
            }]
          },
          options: {
            responsive: true,
            plugins: {
              title: { display: true, text: 'Q1 Responses' },
              legend: { position: 'bottom' }
            }
          }
        });

        const barCtx = document.getElementById('barChart').getContext('2d');
        new Chart(barCtx, {
          type: 'bar',
          data: {
            labels: Object.keys(data.q1_counts),
            datasets: [{
              label: 'Count',
              data: Object.values(data.q1_counts),
              backgroundColor: '#36A2EB'
            }]
          },
          options: {
            responsive: true,
            scales: { y: { beginAtZero: true, title: { display: true, text: 'Count' } } },
            plugins: { legend: { display: false } }
          }
        });

        // Q2 and Q3 Text
        document.getElementById('q2Text').innerHTML = data.q2.map(item => `<li class="list-group-item">${item}</li>`).join('');
        document.getElementById('q3Text').innerHTML = data.q3.map(item => `<li class="list-group-item">${item}</li>`).join('');
      })
      .catch(error => console.error('Error loading charts:', error));
  }
});